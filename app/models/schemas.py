from typing import List, Optional

from pydantic import BaseModel


class DataTypeInfo(BaseModel):
    type_id: str
    display_name: str
    description: Optional[str] = None
    requires_forma: bool = False
    available_formats: Optional[List[str]] = None


class AvailableDataTypesResponse(BaseModel):
    data_types: List[DataTypeInfo]
