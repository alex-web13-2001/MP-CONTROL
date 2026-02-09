"""
EventLog model for tracking advertising events (bid changes, status changes, etc.)
"""
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class EventLog(Base):
    """
    Stores advertising events for timeline visualization.
    
    Event types:
    - BID_CHANGE: CPM/CPC changed
    - STATUS_CHANGE: Campaign paused/started
    - ITEM_ADD: New item added to campaign
    - ITEM_REMOVE: Item removed from campaign
    """
    __tablename__ = "event_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    shop_id = Column(Integer, nullable=False, index=True)
    advert_id = Column(BigInteger, nullable=False, index=True)
    nm_id = Column(BigInteger, nullable=True)  # NULL for campaign-level events
    
    event_type = Column(String(50), nullable=False)
    old_value = Column(Text, nullable=True)
    new_value = Column(Text, nullable=True)
    
    event_metadata = Column(JSON, nullable=True)  # Additional data (renamed: 'metadata' is reserved)
    
    def __repr__(self):
        return f"<EventLog {self.event_type} advert={self.advert_id} at {self.created_at}>"
