# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SearchItem(BaseModel):
    final_rank: int | None = None
    rank: int | None = None
    entity_id: str
    entity_type: str
    name: str
    source: str | None = None
    confidence: float | None = None
    uncertainty_estimate: float | None = None
    uncertainty_interval: list[float] | None = None
    confidence_source: str | None = None
    temperature_max: float | None = None
    strength: float | None = None
    conductivity: float | None = None
    ph: float | None = None
    salinity: float | None = None
    distance: float | None = None
    similarity_estimate: float | None = None
    semantic_text: str | None = None
    query_variant: str | None = None
    retrieval_route: str | None = None
    context_score: float | None = None
    context_fit: str | None = None
    context_reasons: str | None = None


class SearchResponse(BaseModel):
    query: str
    query_language: str | None = None
    normalized_query: str | None = None
    rows: int
    items: list[SearchItem]
    cache: dict[str, Any] | None = None


class SoilContext(BaseModel):
    type: str
    salinity: float
    ph: float


class ClimateContext(BaseModel):
    temp_current: float
    temp_max: float
    rainfall: float
    humidity: float


class AgricultureContext(BaseModel):
    main_crops: list[str]
    irrigation: str


class ContextResponse(BaseModel):
    query_lat: float
    query_lon: float
    location_name: str
    matched_lat: float
    matched_lon: float
    distance_km: float
    soil: SoilContext
    climate: ClimateContext
    agriculture: AgricultureContext
    notes: str
    providers: dict[str, Any] | None = None
    cache: dict[str, Any] | None = None


class RecommendationChoice(BaseModel):
    entity_id: str
    name: str
    context_fit: str | None = None
    reason: str | None = None


class RecommendResponse(BaseModel):
    prompt: str
    prompt_language: str | None = None
    normalized_prompt: str | None = None
    context: ContextResponse
    vector_hits: list[SearchItem] = Field(default_factory=list)
    recommended_combination: dict[str, RecommendationChoice] = Field(default_factory=dict)
    final_recommendations: list[SearchItem] = Field(default_factory=list)
    cache: dict[str, Any] | None = None
