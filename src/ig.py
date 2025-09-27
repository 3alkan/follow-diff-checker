"""Domain models for Instagram-related records.

These are lightweight containers used by the JSON parsers.
"""

class InstagramEvent:
    """Base event data for a single Instagram relation change."""
    def __init__(self, event_type: str, timestamp: str, username: str, profile_url: str):
        self.event_type = event_type
        self.timestamp = timestamp
        self.username = username
        self.profile_url = profile_url

class FollowerChange(InstagramEvent):
    """A user who follows you."""
    def __init__(self, timestamp: str, username: str, profile_url: str):
        super().__init__(event_type="follower_change", timestamp=timestamp, username=username, profile_url=profile_url)

class FollowingChange(InstagramEvent):
    """A user you follow."""
    def __init__(self, timestamp: str, username: str, profile_url: str):
        super().__init__(event_type="following_change", timestamp=timestamp, username=username, profile_url=profile_url)

