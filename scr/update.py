from tqdm import tqdm
from datetime import datetime, timedelta
from IPython.display import clear_output

from scr.loading_bgg import get_users_for_scrap, get_api_user_ratings_data, get_loading_stat_from_db
from scr.users_bgg import parse_user_collection_info, add_to_deleted, upd_checking_date_in_db
from scr.utils import get_user_profile


def daily(PATH_TO_DB: str,
          n_users=1000,
          pre_request=True,
          with_ratings=True,
          check_plans=False,
          stop_after_minutes=0,
          stop_after_hours=0):
    """
    Function for daily update database
    :param PATH_TO_DB: str - path to SQLite database file
    :param n_users: int - number of users for updating
    :param pre_request: bool - if True - do pre request for starting collecting process in API
    :param with_ratings: bool - if True - update users only with ratings in database
    :param check_plans: bool - if True - check number of users
    :return: None
    """
    start_datetime = datetime.today()
    print(f'Started at {start_datetime.isoformat()[11:19]}')
    # Update info for users with ratings
    users = get_users_for_scrap(PATH_TO_DB=PATH_TO_DB, n_users=n_users, with_ratings=with_ratings)
    nicknames = []
    counter = 0
    for user in tqdm(users, desc='Check users'):
        try:
            if get_user_profile(user[0])['last_login'] >= user[1]:
                nicknames.append(user[0])
            else:
                last_check = datetime.today().isoformat()[:10]
                upd_checking_date_in_db(PATH_TO_DB, last_check, user[0])
                counter += 1
        except:
            nicknames.append(user[0])

    if pre_request:
        for nickname in tqdm(nicknames, desc='Preparation'):
            get_api_user_ratings_data(nickname)

    while nicknames:
        for nickname in tqdm(nicknames, desc='Processing'):
            ratings = get_api_user_ratings_data(nickname, return_request=True)
            flag = parse_user_collection_info(PATH_TO_DB, ratings, nickname)
            if flag:
                nicknames.remove(nickname)
            counter += 1
            if datetime.today() > start_datetime + timedelta(minutes=stop_after_minutes, hours=stop_after_hours):
                break
        print(f'Need to recheck {len(nicknames)} users')
        if datetime.today() > start_datetime + timedelta(minutes=stop_after_minutes, hours=stop_after_hours):
            break
    if check_plans:
        num_of_users = get_loading_stat_from_db(PATH_TO_DB, today_loaded=False)[0][0]
        updated_users = get_loading_stat_from_db(PATH_TO_DB, today_loaded=True)[0][0]

    end_datetime = datetime.today()

    clear_output(wait=True)
    print(f'Updated {counter} users in {end_datetime - start_datetime}')
    if check_plans:
        print(f'Total {updated_users} users updated for last 30 days')
        print(f'For actualization - {num_of_users} users')