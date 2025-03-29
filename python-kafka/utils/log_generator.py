import random
import uuid
from datetime import datetime

def generate_log():
    """Tạo log message ngẫu nhiên"""
    log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    services = ["user-service", "payment-service", "inventory-service", "notification-service"]
    actions = ["login", "logout", "purchase", "view", "update", "delete", "register"]
    statuses = ["success", "failure", "pending", "timeout"]
    
    return {
        "timestamp": datetime.now().isoformat(),
        "uuid": str(uuid.uuid4()),
        "level": random.choice(log_levels),
        "service": random.choice(services),
        "action": random.choice(actions),
        "status": random.choice(statuses),
        "responseTime": random.randint(1, 500),
        "userId": f"user-{random.randint(1, 10000)}"
    }
