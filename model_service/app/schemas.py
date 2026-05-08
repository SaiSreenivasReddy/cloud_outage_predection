from pydantic import BaseModel, ConfigDict
from typing import List, Optional
from datetime import datetime


class PredictionRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    cloud_provider: str
    service: str
    severity: str
    system_load_before_outage: Optional[int] = 50
    number_of_customers_affected: int
    ticket_count: int
    backup_system_triggered: str


class BatchPredictionRequest(BaseModel):
    features: List[PredictionRequest]
    source: str = "webapp"


class PredictionResult(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    cloud_provider: str
    service: str
    severity: str
    system_load_before_outage: Optional[int]
    number_of_customers_affected: int
    ticket_count: int
    backup_system_triggered: str
    predicted_hours: float
    is_anomaly: bool
    model_version: str


class BatchPredictionResponse(BaseModel):
    predictions: List[PredictionResult]


class PastPrediction(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    id: int
    timestamp: datetime
    model_version: str
    source: str
    cloud_provider: Optional[str]
    service: Optional[str]
    severity: Optional[str]
    system_load_before_outage: Optional[int]
    number_of_customers_affected: Optional[int]
    ticket_count: Optional[int]
    backup_system_triggered: Optional[str]
    predicted_hours: float
    is_anomaly: bool
