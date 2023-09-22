import os
from datetime import datetime
import sqlite3

import numpy as np
from bs4 import BeautifulSoup


from scr.utils import get_user_profile


def add_user_into_db(nickname: str, PATH_TO_DB: str):
    """
    Function check user nickname in db, and add info about this user if needed
    ====================
    :param nickname: str - user's nickname on BGG.com for scraping
    :param PATH_TO_DB: str - path to SQLite database file
    :return: None
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute('''
                    SELECT u.user_id
                    FROM users u
                    WHERE u.nickname = ?
                    ''', (nickname,))
    finally:
        conn.close()
    if not cursor.fetchall():
        try:
            user_dict = get_user_profile(nickname)

            # get country code
            country_id = np.nan

            if user_dict['country'] != '':
                conn = sqlite3.connect(PATH_TO_DB)
                cursor = conn.cursor()
                try:
                    cursor.execute('''
                                SELECT c.country_id
                                FROM countries c
                                WHERE c.country = ?
                                ''', (user_dict['country'],))
                finally:
                    conn.close()

                country_id = cursor.fetchall()
                if country_id:
                    country_id = country_id[0][0]
                else:
                    print('### No country in DB: ', user_dict['country'])

            website_id = np.nan
            if user_dict['website'] != '':
                conn = sqlite3.connect(PATH_TO_DB)
                cursor = conn.cursor()
                try:
                    cursor.execute('''
                                SELECT l.link_id
                                FROM links l
                                WHERE l.link = ?
                                ''', (user_dict['website'],))
                finally:
                    conn.close()

                website = cursor.fetchall()
                if not website:
                    conn = sqlite3.connect(PATH_TO_DB)
                    cursor = conn.cursor()
                    try:
                        cursor.execute('''
                            INSERT INTO links (link) VALUES (?)
                            ''', (user_dict['website'],))
                        print('Add link to db -> ', user_dict['website'])
                        cursor.execute('''
                                    SELECT l.link_id
                                    FROM links l
                                    WHERE l.link = ?
                                    ''', (user_dict['website'],))
                        website_id = cursor.fetchall()[0][0]
                        conn.commit()
                    finally:
                        conn.close()

            last_check = '2022-12-31'  # date.today()

            conn = sqlite3.connect(PATH_TO_DB)
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO users (nickname, country_id, website_id, 
                    registration_date, last_profile_update, last_login, last_check) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                    nickname, country_id, website_id, user_dict['registration_date'], user_dict['last_profile_update'],
                    user_dict['last_login'], last_check))
                conn.commit()
            finally:
                conn.close()

        except:
            print(f'### Check {nickname} manually')


def get_comment_id(PATH_TO_DB: str, comment: str) -> int:
    """
    Function for getting comment id from database and add comment to database if it's needed
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param comment: str - user's comment
    :return: int - comment id in database
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()  # create a cursor object
    try:
        cursor.execute(f'''
                        SELECT c.comment_id
                        FROM comments c
                        WHERE c.comment = ?
                        ''', (comment,))
        data = cursor.fetchall()
    finally:
        conn.close()  # close the connection

    if data:
        comment_id = data[0][0]
    else:
        conn = sqlite3.connect(PATH_TO_DB)
        cursor = conn.cursor()  # create a cursor object
        try:
            cursor.execute('''
                            INSERT INTO comments (comment) VALUES (?)
                            ''', (comment,))
            conn.commit()
        finally:
            conn.close()  # close the connection

        # get saved comment's id
        conn = sqlite3.connect(PATH_TO_DB)
        cursor = conn.cursor()  # create a cursor object
        try:
            cursor.execute(f'''
                            SELECT c.comment_id
                            FROM comments c
                            WHERE c.comment = ?
                            ''', (comment,))
            comment_id = cursor.fetchall()[0][0]
        finally:
            conn.close()  # close the connection

    return int(comment_id)


def get_user_id(PATH_TO_DB: str, nickname: str) -> int:
    """
    Get user_id from database using nickname
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param nickname: str - user nickname, his/her profile will be checked on boardgamegeek.com
    :return: user_id - int
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()  # create a cursor object
    try:
        cursor.execute(f'''
                        SELECT u.user_id
                        FROM users u
                        WHERE u.nickname = ?
                        ''', (nickname,))
        user_id = cursor.fetchall()[0][0]
    except:
        print(f"User ID for user {user_id} is invalid")
        user_id = np.nan
    finally:
        conn.close()  # close the connection
    return int(user_id)


def add_rating_into_db(PATH_TO_DB: str,
                       user_id: int,
                       boardgame_id: int,
                       rating: float,
                       num_of_plays: int,
                       comment_id: int,
                       own: int,
                       prevowned: int,
                       for_trade: int,
                       want: int,
                       want_to_play: int,
                       want_to_buy: int,
                       wishlist: int,
                       preordered: int,
                       last_modified: str):
    """
    Function for add rating in database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param user_id: int - user id from database
    :param boardgame_id: int - boardgame id from database
    :param rating: float - rating which user add for boardgame
    :param num_of_plays: int - user's number of plays in this boardgame
    :param comment_id: int - id of comment which user add for this game
    :param own: int - own status flag, 0 or 1
    :param prevowned: int - prevowned status flag, 0 or 1
    :param for_trade: int - for_trade status flag, 0 or 1
    :param want: int - want status flag, 0 or 1
    :param want_to_play: int - want_to_play status flag, 0 or 1
    :param want_to_buy: int - want_to_buy status flag, 0 or 1
    :param wishlist: int - wishlist status flag, 0 or 1
    :param preordered: int - preordered status flag, 0 or 1
    :param last_modified: str - date of last modifying rating in iso format
    :return: None
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO ratings (user_id, boardgame_id, rating, 
            num_of_plays, comment_id, own, prevowned, for_trade, 
            want, want_to_play, want_to_buy, wishlist, preordered, 
            last_modified) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, boardgame_id, rating, num_of_plays,
                  comment_id, own, prevowned, for_trade, want,
                  want_to_play, want_to_buy, wishlist, preordered,
                  last_modified))
        conn.commit()
    finally:
        conn.close()


def upd_rating_in_db(PATH_TO_DB: str,
                     num_of_plays: int,
                     comment_id: int,
                     own: int,
                     prevowned: int,
                     for_trade: int,
                     want: int,
                     want_to_play: int,
                     want_to_buy: int,
                     wishlist: int,
                     preordered: int,
                     rating_id: int):
    """
    Function for updating rating in database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param num_of_plays: int - user's number of plays in this boardgame
    :param comment_id: int - id of comment which user add for this game
    :param own: int - own status flag, 0 or 1
    :param prevowned: int - prevowned status flag, 0 or 1
    :param for_trade: int - for_trade status flag, 0 or 1
    :param want: int - want status flag, 0 or 1
    :param want_to_play: int - want_to_play status flag, 0 or 1
    :param want_to_buy: int - want_to_buy status flag, 0 or 1
    :param wishlist: int - wishlist status flag, 0 or 1
    :param preordered: int - preordered status flag, 0 or 1
    :param rating_id: int - rating id for updating
    :return: None
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
                    UPDATE ratings 
                    SET num_of_plays = ?, comment_id = ?, own = ?, 
                        prevowned = ?, for_trade = ?, want = ?, 
                        want_to_play = ?, want_to_buy = ?, wishlist = ?, 
                        preordered = ?
                    WHERE rating_id = ?
                    """,
                       (num_of_plays, comment_id, own, prevowned, for_trade,
                        want, want_to_play, want_to_buy, wishlist, preordered,
                        rating_id))
        conn.commit()
    finally:
        conn.close()


def upd_checking_date_in_db(PATH_TO_DB: str,
                            last_check: str,
                            nickname: str):
    """
    Function for updating last check date in database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param last_check: str - date of last checking user in iso format
    :param nickname: str - user's nickname on BGG.com for scraping
    :return: None
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
                    UPDATE users 
                    SET last_check = ? 
                    WHERE nickname = ?
                    """,
                       (last_check, nickname))
        conn.commit()
    finally:
        conn.close()


def get_rating_id(PATH_TO_DB: str,
                  user_id: int,
                  boardgame_id: int,
                  rating: float,
                  last_modified: str):
    """
    Function for getting rating id from database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param user_id: int - user id from database
    :param boardgame_id: int - boardgame id from database
    :param rating: float - rating which user add for boardgame
    :param last_modified: str - date of last modifying rating in iso format
    :return: list of tuples with rating id
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
                            SELECT rating_id, num_of_plays
                            FROM ratings
                            WHERE (user_id = ?) AND (boardgame_id = ?) AND (rating = ?) AND (last_modified = ?)
                            """,
                       (user_id, boardgame_id, rating, last_modified))
        rating_info = cursor.fetchall()
    finally:
        conn.close()
    return rating_info


def parse_status(status):
    """
    Function for parsing date of last modifying rating and all status flags
    ====================
    :param status: BS4-file from xml with user boardgame's status
    :return: Date of last modifying and all status flags
    """
    last_modified = status.get('lastmodified')[:10]
    own = int(status.get('own'))
    prevowned = int(status.get('prevowned'))
    for_trade = int(status.get('fortrade'))
    want = int(status.get('want'))
    want_to_play = int(status.get('wanttoplay'))
    want_to_buy = int(status.get('wanttobuy'))
    wishlist = int(status.get('wishlist'))
    preordered = int(status.get('preordered'))
    return last_modified, own, prevowned, for_trade, want, want_to_play, \
        want_to_buy, wishlist, preordered


def add_to_deleted(PATH_TO_DB: str,
                   nickname: str):
    """
    Function for additing 'deleted' flag for user into SQLite database
    for user's with deleted account on BGG.com
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param nickname: str - user's nickname on BGG.com for scraping
    :return: None
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
                    UPDATE users 
                    SET deleted = 1
                    WHERE nickname = ?
                    """,
                       (nickname, ))
        conn.commit()
    finally:
        conn.close()


def parse_item(PATH_TO_DB, item, user_id):
    """
    Function for parsing user's rating and saving it into SQLite database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param item: BS4-file from xml with user's rating
    :param user_id: user's id in database
    :return: None
    """
    boardgame_id = int(item.get('objectid'))

    rating = float(item.find('stats').find('rating').get('value'))
    num_of_plays = int(item.find('numplays').text)
    try:
        comment = item.find('comment').text
        comment_id = get_comment_id(PATH_TO_DB=PATH_TO_DB, comment=comment)
    except:
        comment_id = None
    status = item.find('status')

    # Parse status from xml
    last_modified, own, prevowned, for_trade, want, want_to_play, \
        want_to_buy, wishlist, preordered = parse_status(status)

    rating_info = get_rating_id(PATH_TO_DB, user_id, boardgame_id,
                                rating, last_modified)

    if rating_info:
        if num_of_plays != int(rating_info[0][1]):
            rating_id = int(rating_info[0][0])
            # Update data into db
            upd_rating_in_db(PATH_TO_DB, num_of_plays, comment_id, own,
                             prevowned, for_trade, want, want_to_play,
                             want_to_buy, wishlist, preordered, rating_id)
    else:
        # Add data into db
        add_rating_into_db(PATH_TO_DB, user_id, boardgame_id, rating, num_of_plays,
                           comment_id, own, prevowned, for_trade, want,
                           want_to_play, want_to_buy, wishlist, preordered,
                           last_modified)


def parse_user_collection_info(PATH_TO_DB: str,
                               ratings,
                               nickname: str,
                               PATH_TO_READ=None,
                               PATH_TO_SAVE=None,
                               PATH_TO_DEL=None):
    """
    Function for parsing user's ratings and saving them into SQLite database
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param ratings: BS4-file from xml with user's ratings
    :param nickname: str - user's nickname on BGG.com for scraping
    :param PATH_TO_READ: str - path to dir with scraped .xml user ratings files
    :param PATH_TO_SAVE: str - path to dir for saving scraped .xml files for users with ratings
    :param PATH_TO_DEL: str - path to dir for saving scraped .xml files for users without ratings
    :return: None
    """
    try:
        total_ratings = int(ratings.find('items').get('totalitems'))
    except:
        message = str(ratings.find('message').text)
        if message == 'Invalid username specified':
            add_to_deleted(PATH_TO_DB=PATH_TO_DB, nickname=nickname)
            return True
        else:
            return False

    if total_ratings > 0:
        items = ratings.find_all('item')
        user_id = get_user_id(PATH_TO_DB=PATH_TO_DB, nickname=nickname)
        for item in items:
            parse_item(PATH_TO_DB, item, user_id)

        if PATH_TO_SAVE:
            os.replace(PATH_TO_READ + '/' + nickname + '.xml',  # move file from buffer dir
                       PATH_TO_SAVE + '/' + nickname + '.xml')  # to saving dir
    elif PATH_TO_DEL:
        os.replace(PATH_TO_READ + '/' + nickname + '.xml',  # move file from buffer dir
                   PATH_TO_DEL + '/' + nickname + '.xml')  # to temporary dir

    # Save checking date into db
    last_check = datetime.strptime(str(ratings.find('items').get('pubdate')),
                                   '%a, %d %b %Y %H:%M:%S %z').isoformat()[:10]

    upd_checking_date_in_db(PATH_TO_DB, last_check, nickname)
    return True


def parse_loaded_users_info(PATH_TO_DB: str,
                            PATH_TO_READ: str,
                            PATH_TO_SAVE: str,
                            PATH_TO_DEL: str):
    """
    Function for parsing loaded .xml user ratings files after scraping
    ====================
    :param PATH_TO_DB: str - path to SQLite database file
    :param PATH_TO_READ: str - path to dir with scraped .xml user ratings files
    :param PATH_TO_SAVE: str - path to dir for saving scraped .xml files for users with ratings
    :param PATH_TO_DEL: str - path to dir for saving scraped .xml files for users without ratings
    :return:
    """
    files = os.listdir(PATH_TO_READ)
    for user_file in files:
        if user_file == '.DS_Store':
            continue

        f = open(PATH_TO_READ + '/' + user_file, 'r')
        ratings = BeautifulSoup(f.read(), 'xml')
        nickname = user_file.replace('.xml', '')

        parse_user_collection_info(PATH_TO_DB, ratings, nickname,
                                   PATH_TO_READ, PATH_TO_SAVE, PATH_TO_DEL)
