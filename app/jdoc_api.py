import httpx
from typing import List, Dict, Optional
from .config import settings
from .auth import auth
from .database import db
from app.models import NormalizedOperation

class JDOCClient:
    """Client for interacting with John Deere Operations Center API"""
    
    def __init__(self):
        self.base_url = settings.api_base_url
    
    async def _make_request(self, user_id: str, endpoint: str, method: str = "GET", **kwargs) -> Dict:
        """
        Make authenticated request to JDOC API
        
        Args:
            user_id: User identifier
            endpoint: API endpoint (e.g., '/organizations')
            method: HTTP method
            **kwargs: Additional arguments for httpx request
            
        Returns:
            API response as dictionary
        """
        access_token = await auth.get_valid_token(user_id)
        
        if not access_token:
            raise Exception("No valid access token available")
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/vnd.deere.axiom.v3+json',
            **kwargs.pop('headers', {})
        }
        
        url = f"{self.base_url}{endpoint}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(method, url, headers=headers, **kwargs)
            
            if response.status_code == 401:
                raise Exception("Authentication failed - token may be invalid")
            elif response.status_code == 403:
                raise Exception("Access forbidden - check organization permissions")
            elif response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
            
            return response.json()
    
    async def get_organizations(self, user_id: str) -> List[Dict]:
        """
        Get all organizations connected to this user
        
        Returns:
            List of organization dictionaries
        """
        response = await self._make_request(user_id, '/organizations')
        
        organizations = response.get('values', [])
        
        # Save organizations to database
        for org in organizations:
            db.save_organization(user_id, org)
        
        return organizations
    
    async def check_connections_needed(self, user_id: str) -> Optional[str]:
        """
        Check if user needs to enable organization connections
        
        Returns:
            Connection URL if needed, None otherwise
        """
        orgs = await self.get_organizations(user_id)
        
        for org in orgs:
            for link in org.get('links', []):
                if link.get('rel') == 'connections':
                    # User needs to enable org access
                    return link.get('uri')
        
        return None
    
    async def get_fields(self, user_id: str, org_id: str, include_boundaries: bool = True) -> List[Dict]:
        """
        Get all fields for an organization
        
        Args:
            user_id: User identifier
            org_id: Organization ID
            include_boundaries: Include boundary data
            
        Returns:
            List of field dictionaries
        """
        endpoint = f'/organizations/{org_id}/fields'
        
        if include_boundaries:
            endpoint += '?embed=boundaries'
        
        response = await self._make_request(user_id, endpoint)
        return response.get('values', [])
    
    async def get_field_operations(
        self, 
        user_id: str, 
        org_id: str, 
        field_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get field operations for a specific field
        
        Args:
            user_id: User identifier
            org_id: Organization ID
            field_id: Field ID
            start_date: Start date in ISO format (e.g., '2020-01-01T00:00:00.000Z')
            end_date: End date in ISO format
            
        Returns:
            List of field operation dictionaries
        """
        endpoint = f'/organizations/{org_id}/fields/{field_id}/fieldOperations'
        
        params = {}
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date
        
        response = await self._make_request(user_id, endpoint, params=params)
        return response.get('values', [])

# Global JDOC client instance
jdoc_client = JDOCClient()




# ============================================
# HELPER FUNCTION: Normalize JDOC Operations
# ============================================

def normalize_operation(raw_operation: dict, field_id: str, field_name: str, org_id: str, org_name: str) -> "NormalizedOperation":
    """
    Convert a raw JDOC fieldOperation into AgriCapture's normalized format
    
    Args:
        raw_operation: Raw JDOC fieldOperation JSON object
        field_id: The field this operation belongs to
        field_name: Name of the field
        org_id: Organization ID
        org_name: Organization name
        
    Returns:
        NormalizedOperation with extracted/mapped fields
    """
    from app.models import NormalizedOperation
    from datetime import datetime
    
    # Extract operation type (JDOC uses "type" or "operationType")
    op_type = raw_operation.get("type") or raw_operation.get("operationType", "OTHER")
    
    # Get the start time (when the operation happened)
    start_time = raw_operation.get("startTime")
    if start_time:
        # Parse ISO format timestamp
        if isinstance(start_time, str):
            try:
                date = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except:
                date = datetime.now()
        else:
            date = start_time
    else:
        date = datetime.now()
    
    # Extract crop name if available
    crop_name = None
    if "crop" in raw_operation:
        crop_info = raw_operation.get("crop", {})
        if isinstance(crop_info, dict):
            crop_name = crop_info.get("name")
        else:
            crop_name = str(crop_info)
    
    # Extract area
    area = None
    area_unit = None
    if "area" in raw_operation:
        area_obj = raw_operation.get("area", {})
        if isinstance(area_obj, dict):
            area = area_obj.get("valueAsDouble") or area_obj.get("value")
            area_unit = area_obj.get("unit", "ha")
    
    # Extract product and rate info (for fertilizer/spray operations)
    product_name = None
    amount = None
    rate = None
    rate_unit = None
    
    if "resources" in raw_operation:
        resources = raw_operation.get("resources", [])
        if resources and len(resources) > 0:
            first_resource = resources[0]
            if isinstance(first_resource, dict):
                product_name = first_resource.get("product", {}).get("name")
                amount = first_resource.get("quantity", {}).get("valueAsDouble")
                rate = first_resource.get("rate", {}).get("rateAsDouble")
                rate_unit = first_resource.get("rate", {}).get("unit")
    
    # Create and return normalized operation
    return NormalizedOperation(
        field_id=field_id,
        field_name=field_name,
        org_id=org_id,
        org_name=org_name,
        operation_type=op_type,
        date=date,
        crop_name=crop_name,
        product_name=product_name,
        amount=amount,
        rate=rate,
        rate_unit=rate_unit,
        area=area,
        area_unit=area_unit,
        raw_jdoc_data=raw_operation  # Keep original for debugging
    )


def build_leaf_like_hierarchy(
    farmer_id: str,
    organizations_raw: list,
    fields_with_boundaries: dict,  # {org_id: {field_id: field_obj}}
    operations_normalized: dict     # {field_id: [normalized_ops]}
) -> list:
    """
    Build Leaf-like hierarchy: Organization → Farm → Field → Boundaries/Operations
    
    Args:
        farmer_id: Farmer identifier
        organizations_raw: Raw JDOC organizations
        fields_with_boundaries: Dict mapping org_id -> field_id -> field data
        operations_normalized: Dict mapping field_id -> normalized operations
        
    Returns:
        List of Organization objects in Leaf-like format
    """
    from app.models import Organization, Farm, Field, Boundary, NormalizedOperation
    
    orgs_list = []
    
    for org_raw in organizations_raw:
        org_id = org_raw.get("id")
        org_name = org_raw.get("name", org_id)
        
        # Get all fields for this org
        fields_for_org = fields_with_boundaries.get(org_id, {})
        
        # For now, treat all fields as being in one "default farm"
        # (JDOC doesn't have explicit farms, but Leaf does)
        default_farm = Farm(
            id=f"{org_id}-farm-default",
            name=f"{org_name} Farm",
            fields=[]
        )
        
        for field_id, field_raw in fields_for_org.items():
            # Build boundaries for this field
            boundaries = []
            if "boundaries" in field_raw:
                for boundary_raw in field_raw.get("boundaries", []):
                    boundary = Boundary(
                        id=boundary_raw.get("id", "unknown"),
                        name=boundary_raw.get("name"),
                        geometry=extract_geojson(boundary_raw),  # Will define this next
                        area=extract_area(boundary_raw),
                        area_unit="ha",
                        active=boundary_raw.get("active", True)
                    )
                    boundaries.append(boundary)
            
            # Get operations for this field
            field_operations = operations_normalized.get(field_id, [])
            
            # Build field object
            field = Field(
                id=field_id,
                name=field_raw.get("name", field_id),
                boundaries=boundaries,
                operations=field_operations
            )
            
            default_farm.fields.append(field)
        
        # Build organization object
        org = Organization(
            id=org_id,
            name=org_name,
            type=org_raw.get("type", "unknown"),
            farms=[default_farm] if default_farm.fields else []
        )
        
        orgs_list.append(org)
    
    return orgs_list


def extract_geojson(boundary_raw: dict) -> Optional[dict]:
    """Extract GeoJSON geometry from JDOC boundary"""
    if "multipolygons" in boundary_raw:
        # JDOC uses multipolygons with points
        multipolygons = boundary_raw.get("multipolygons", [])
        if multipolygons:
            polygon = multipolygons[0]
            if "rings" in polygon:
                rings = polygon.get("rings", [])
                if rings:
                    ring = rings[0]
                    points = ring.get("points", [])
                    # Convert points to GeoJSON coordinate format [lon, lat]
                    coordinates = [
                        [point.get("lon"), point.get("lat")] 
                        for point in points
                    ]
                    return {
                        "type": "Polygon",
                        "coordinates": [coordinates]
                    }
    
    return None


def extract_area(boundary_raw: dict) -> Optional[float]:
    """Extract area from JDOC boundary"""
    if "area" in boundary_raw:
        area_obj = boundary_raw.get("area", {})
        if isinstance(area_obj, dict):
            return area_obj.get("valueAsDouble")
    
    return None
