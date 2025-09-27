from ..ig import FollowerChange, FollowingChange, PendingFollowRequestChange

def parse_followers(data:dict)->dict[str, FollowerChange]:
    """Parse follower data into a list of FollowerChange objects."""
    followers = {}
    
    return followers