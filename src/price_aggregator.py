from typing import Dict, Any, Optional

class PriceAggregator:
    
    def __init__(self):
        self.total_price = 0.0
        self.count = 0
        self.running_average = 0.0
    
    def update(self, price: float) -> float:
        self.total_price += price
        self.count += 1
        self.running_average = self.total_price / self.count
        return self.running_average
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_orders': self.count,
            'total_revenue': round(self.total_price, 2),
            'running_average': round(self.running_average, 2)
        }