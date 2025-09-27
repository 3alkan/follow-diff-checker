class InstagramEvent:
    def __init__(self, event_type: str, timestamp: str, username: str, profile_url: str):
        self.event_type = event_type
        self.timestamp = timestamp
        self.username = username
        self.profile_url = profile_url

class FollowerChange(InstagramEvent):
    def __init__(self, timestamp: str, username: str, profile_url: str):
        super().__init__(event_type="follower_change", timestamp=timestamp, username=username, profile_url=profile_url)

class FollowingChange(InstagramEvent):
    def __init__(self, timestamp: str, username: str, profile_url: str):
        super().__init__(event_type="following_change", timestamp=timestamp, username=username, profile_url=profile_url)

class PendingFollowRequestChange(InstagramEvent):
    def __init__(self, timestamp: str, username: str, profile_url: str):
        super().__init__(event_type="pending_follow_request_change", timestamp=timestamp, username=username, profile_url=profile_url)