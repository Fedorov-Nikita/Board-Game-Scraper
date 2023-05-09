from tqdm import tqdm

from scr.loading_bgg import get_users_for_scrap, get_api_user_ratings_data
from scr.users_bgg import parse_user_collection_info, add_to_deleted
from scr.utils import get_user_profile


def daily(PATH_TO_DB: str,
          n_users=1000,
          pre_request=True,
          with_ratings=True):
    """
    Function for daily update database
    :param PATH_TO_DB: str - path to SQLite database file
    :param n_users: int - number of users for updating
    :param pre_request: bool - if True - do pre request for starting collecting process in API
    :param with_ratings: bool - if True - update users only with ratings in database
    :return: None
    """
    # Update info for users with ratings
    users = get_users_for_scrap(PATH_TO_DB=PATH_TO_DB, n_users=n_users, with_ratings=with_ratings)
    nicknames = []
    for user in tqdm(users):
        try:
            if get_user_profile(user[0])['last_login'] >= user[1]:
                nicknames.append(user[0])
        except:
            nicknames.append(user[0])

    if pre_request:
        for nickname in tqdm(nicknames):
            get_api_user_ratings_data(nickname)

    while nicknames:
        for nickname in tqdm(nicknames):
            ratings = get_api_user_ratings_data(nickname, return_request=True)
            flag = parse_user_collection_info(PATH_TO_DB, ratings, nickname)
            if flag:
                nicknames.remove(nickname)
        print(f'Need to recheck {nicknames}')