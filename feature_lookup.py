from dataclasses import dataclass
from typing import List, Optional

@dataclass
class FeatureLookup:
    table_name: str
    feature_names: List[str]
    lookup_key: str
    timestamp_key: Optional[str] = None
    
    def __repr__(self):
        repr_str = f"FeatureLookup(\n  table_name='{self.table_name}',\n  feature_names={self.feature_names},\n  lookup_key='{self.lookup_key}'"
        if self.timestamp_key:
            repr_str += f",\n  timestamp_key='{self.timestamp_key}'"
        repr_str += "\n)"
        return repr_str

# Global variable to store feature lookups
feature_lookups = [] 