class ReferralEngineError(Exception):
    pass


class CycleDetectedError(ReferralEngineError):
    def __init__(self, user_id: int, referrer_id: int) -> None:
        self.user_id = user_id
        self.referrer_id = referrer_id
        super().__init__(
            f"Cycle detected: assigning user {user_id} → referrer {referrer_id}"
        )


class UserNotFoundError(ReferralEngineError):
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id
        super().__init__(f"User not found: {user_id}")


class DuplicateReferralError(ReferralEngineError):
    pass


class AdapterError(ReferralEngineError):
    pass