from src.ig import FollowerChange, FollowingChange

def parse_followers(data:list)->dict[str, FollowerChange]:
    """Parse follower data into a list of FollowerChange objects."""
    try:
        followers = {}
        for item in data:
            item=dict(item)
            item_data:list=item.get('string_list_data')
            if item_data:
                record:dict=item_data[0]
                ts:int=record.get('timestamp')
                username:str=record.get('value')
                profile_url:str=record.get('href')
                if ts and username and profile_url:
                    followers[username] = FollowerChange(
                        username=username,
                        profile_url=profile_url,
                        timestamp=ts
                    )
        return followers
    except Exception as e:
        print(f"Error parsing followers: {e}")
        return {}

def parse_followings(data:dict)->dict[str, FollowingChange]:
    """Parse following data into a list of FollowingChange objects."""
    try:
        followings = {}
        data:list = data.get('relationships_following')
        if data:
            for item in data:
                item=dict(item)
                item_data:list=item.get('string_list_data')
                if item_data:
                    record:dict=item_data[0]
                    ts:int=record.get('timestamp')
                    username:str=record.get('value')
                    profile_url:str=record.get('href')
                    if ts and username and profile_url:
                        followings[username] = FollowingChange(
                            username=username,
                            profile_url=profile_url,
                            timestamp=ts
                        )
        return followings
    except Exception as e:
        print(f"Error parsing followings: {e}")
        return {}
