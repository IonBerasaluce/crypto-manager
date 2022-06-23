from typing import Optional
from typing import Optional
from data.mongo_user import User

import services.data_service as svc

active_user: Optional[User] = None

def reload_account():
    global active_user
    if not active_user:
        return
    
    active_user = svc.find_account_by_email(active_user.user_email)
