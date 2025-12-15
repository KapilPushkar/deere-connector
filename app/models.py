from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class NormalizedOperation(BaseModel):
    """Normalized operation data that AgriCapture expects"""
    
    # Basic identifiers
    field_id: str
    field_name: str
    org_id: str
    org_name: str
    
    # Operation classification
    operation_type: str  # "PLANTING", "HARVEST", "TILLAGE", "FERTILIZER", "OTHER"
    
    # Timing
    date: datetime  # When the operation happened
    
    # Crop/Product info
    crop_name: Optional[str] = None  # e.g., "Corn", "Soybeans"
    product_name: Optional[str] = None  # e.g., "Urea", "Anhydrous Ammonia"
    
    # Quantities
    amount: Optional[float] = None  # How much was applied
    rate: Optional[float] = None  # Rate of application
    rate_unit: Optional[str] = None  # e.g., "kg/ha", "gal/acre"
    
    # Area information
    area: Optional[float] = None  # Area covered
    area_unit: Optional[str] = None  # "ha" or "acre"
    
    # Raw data for fallback
    raw_jdoc_data: Optional[dict] = None  # Store original JDOC response for reference

    class Config:
        json_schema_extra = {
            "example": {
                "field_id": "a90cea41-14ba-489f-a7ff-f46c2590733b",
                "field_name": "North Field",
                "org_id": "569776",
                "org_name": "API Testing",
                "operation_type": "PLANTING",
                "date": "2024-03-15T08:00:00Z",
                "crop_name": "Corn",
                "product_name": None,
                "amount": None,
                "rate": None,
                "area": 100.5,
                "area_unit": "ha"
            }
        }


class GeoJSONGeometry(BaseModel):
    """GeoJSON geometry object"""
    type: str  # "Polygon", "MultiPolygon", etc.
    coordinates: list  # [[[lon, lat], [lon, lat], ...]]

class Boundary(BaseModel):
    """Field boundary with geometry"""
    id: str
    name: Optional[str] = None
    geometry: Optional[dict] = None  # GeoJSON geometry
    area: Optional[float] = None
    area_unit: Optional[str] = "ha"
    active: bool = True

class Field(BaseModel):
    """Field with boundaries and operations"""
    id: str
    name: str
    boundaries: list[Boundary] = []
    operations: list[NormalizedOperation] = []

class Farm(BaseModel):
    """Farm containing multiple fields"""
    id: Optional[str] = None
    name: str
    fields: list[Field] = []

class Organization(BaseModel):
    """Organization (grower) with farms and fields"""
    id: str
    name: str
    type: str
    farms: list[Farm] = []
    
    def get_all_fields(self) -> list[Field]:
        """Flatten all fields from all farms"""
        all_fields = []
        for farm in self.farms:
            all_fields.extend(farm.fields)
        return all_fields

class FarmerSnapshot(BaseModel):
    """Complete snapshot of a farmer's data in Leaf-like hierarchy"""
    farmer_id: str
    organizations: list[Organization] = []
    sync_info: dict
    total_fields: int = 0
    total_operations: int = 0
