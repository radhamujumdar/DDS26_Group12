"""Shared HTTP-layer response models."""

from pydantic import BaseModel


class MessageResponse(BaseModel):
    msg: str
