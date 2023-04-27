import time
import requests as re
from bs4 import BeautifulSoup
from tqdm import tqdm
import sqlite3


def get_api_bgg_game_data(unique_ids: list, save_path: str):
    """
    This function retrieves user ratings information via the boardgamegeek.com API
    ====================
    :param unique_ids: list - list of unique boardgames ids (it is best to feed a list of no more than 50 id)
    :param save_path: str - path for saving scraped boardgame's files
    :return: None
    ====================
    BoardGameGeek API:
    https://api.geekdo.com/xmlapi/boardgame/37111?stats=1&pricehistory=1&marketplace=1&comments=1

    base - https://api.geekdo.com/xmlapi/boardgame
    game - /37111 - gameid
    params - ?stats=1&pricehistory=1&marketplace=1&comments=1
    comments: Show brief user comments on games (set it to 1, absent by default)
    stats: Include game statistics (set it to 1, absent by default)
    historical: Include historical game statistics (set it to 1, absent by default) - Use from/end parameters to set starting and ending dates. Returns all data starting from 2006-03-18.
    from: Set the start date to include historical data (format: YYYY-MM-DD, absent by default )
    to: Set the end date to include historical data (format: YYYY-MM-DD, absent by default )
    pricehistory: retrieve the marketplace history for this item (set it to 1, absent by default)
    marketplace: retrieve the current marketplace listings (set it to 1, absent by default)
    """
    url_id = ''
    for i in range(len(unique_ids)):
        if i == 0:
            url_id = str(unique_ids[i])
        else:
            url_id += ',' + str(unique_ids[i])

    api_boardgame = 'https://api.geekdo.com/xmlapi/boardgame/'
    api_params = '?stats=1&pricehistory=1&marketplace=1&comments=1'
    r = re.get(api_boardgame + url_id + api_params)
    soup = BeautifulSoup(r.text, features='xml')
    list_bg = soup.find_all('boardgame')
    for bg in list_bg:
        boardgame_id = bg.get('objectid')
        with open(save_path + '/' + str(boardgame_id) + '.xml', 'w', encoding='utf-8') as f:
            f.write(str(bg))


def get_api_user_ratings_data(nickname, PATH_TO_SAVE=None):
    """
    This function takes the boardgamegeek.com username as input
    and returns a pandas DataFrame with all of the user's scores
    ====================
    :param nickname: str - user nickname for getting ratings
    :param PATH_TO_SAVE: str - path for saving scraped user ratings files
    :return: None
    """
    url_coll_main = 'https://api.geekdo.com/xmlapi/collection/'
    params = '?rated=1'

    time.sleep(0.33)
    r = re.get(url_coll_main + nickname + params)

    if PATH_TO_SAVE:
        soup = BeautifulSoup(r.text, features="xml")
        with open(PATH_TO_SAVE + '/' + str(nickname) + '.xml', 'w', encoding='utf-8') as f:
            f.write(str(soup))


def get_users_for_scrap(PATH_TO_DB: str, n_users=10000, with_ratings=True) -> list:
    """
    This function get you users nicknames for scraping algorithm
    ====================
    :param PATH_TO_DB: str - path to database
    :param n_users: int - number of users which you will get from database
    :return: list of nicknames
    """
    if with_ratings:
        join_type = 'INNER'
    else:
        join_type = 'LEFT OUTER'

    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    cursor.execute(f'''
                SELECT u.nickname
                FROM users u
                {join_type} JOIN (SELECT DISTINCT user_id
                            FROM ratings) r
                ON u.user_id = r.user_id
                ORDER BY u.last_check ASC
                WHERE u.deleted = 0
                LIMIT {n_users}
                ''')
    data = cursor.fetchall()
    conn.close()

    nicknames = []
    for i in data:
        nicknames.append(i[0])
    return nicknames


def load_stack_of_users_to_buffer(PATH_TO_DB: str, PATH_TO_SAVE: str, n_users=10000, pre_request=True):
    """
    This function load user ratings data for the users with the oldest update date
    ====================
    :param PATH_TO_DB:
    :param PATH_TO_SAVE:
    :param n_users:
    :param pre_request:
    :return: None
    """
    nicknames = get_users_for_scrap(PATH_TO_DB=PATH_TO_DB, n_users=n_users)
    if pre_request:
        for nickname in tqdm(nicknames):
            get_api_user_ratings_data(nickname)
    for nickname in tqdm(nicknames):
        get_api_user_ratings_data(nickname, PATH_TO_SAVE=PATH_TO_SAVE)


