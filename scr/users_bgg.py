import os
from datetime import datetime

import numpy as np
import sqlite3
from bs4 import BeautifulSoup

from scr.utils import get_user_profile


def add_user_into_db(nickname: str, PATH_TO_DB: str):
    """
    Function check user nickname in db, and addd info about this user if needed
    ====================
    :param nickname: str - user nickname, his/her profile will be checked on boardgamegeek.com
    :param PATH_TO_DB: str - path to local bg_database (sqlite3)
    :return:
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()

    cursor.execute('''
					SELECT u.user_id
					FROM users u
					WHERE u.nickname = ?
					''', (nickname,))
    conn.close()
    if not cursor.fetchall():
        try:
            user_dict = get_user_profile(nickname)

            # get country code
            country_id = np.nan

            if user_dict['country'] != '':
                conn = sqlite3.connect(PATH_TO_DB)
                cursor = conn.cursor()
                cursor.execute('''
                            SELECT c.country_id
                            FROM countries c
                            WHERE c.country = ?
                            ''', (user_dict['country'],))
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
                cursor.execute('''
                                SELECT l.link_id
                                FROM links l
                                WHERE l.link = ?
                                ''', (user_dict['website'],))
                conn.close()

                website = cursor.fetchall()
                if not website:
                    conn = sqlite3.connect(PATH_TO_DB)
                    cursor = conn.cursor()
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
                    conn.close()

            last_check = '2022-12-31'  # date.today()

            conn = sqlite3.connect(PATH_TO_DB)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO users (nickname, country_id, website_id, 
                registration_date, last_profile_update, last_login, last_check) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                nickname, country_id, website_id, user_dict['registration_date'], user_dict['last_profile_update'],
                user_dict['last_login'], last_check))
            conn.commit()
            conn.close()

        except:
            print(f'### Check {nickname} manually')


def get_comment_id(PATH_TO_DB: str, comment: str) -> int:
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()  # create a cursor object

    cursor.execute(f'''
                    SELECT c.comment_id
                    FROM comments c
                    WHERE c.comment = ?
                    ''', (comment,))
    data = cursor.fetchall()
    conn.close()  # close the connection
    if data:
        comment_id = data[0][0]
    else:
        conn = sqlite3.connect(PATH_TO_DB)
        cursor = conn.cursor()  # create a cursor object
        cursor.execute('''
                        INSERT INTO comments (comment) VALUES (?)
                        ''', (comment,))
        conn.commit()
        conn.close()  # close the connection

        # get saved comment's id
        conn = sqlite3.connect(PATH_TO_DB)
        cursor = conn.cursor()  # create a cursor object
        cursor.execute(f'''
                        SELECT c.comment_id
                        FROM comments c
                        WHERE c.comment = ?
                        ''', (comment,))
        comment_id = cursor.fetchall()[0][0]
        conn.close()  # close the connection

    return int(comment_id)


def get_user_id(PATH_TO_DB: str, nickname: str) -> int:
    """
    Get user_id from database using nickname
    ====================
    :param PATH_TO_DB: str - path to local bg_database (sqlite3)
    :param nickname: str - user nickname, his/her profile will be checked on boardgamegeek.com
    :return: user_id - int
    """
    conn = sqlite3.connect(PATH_TO_DB)
    cursor = conn.cursor()  # create a cursor object

    cursor.execute(f'''
                    SELECT u.user_id
                    FROM users u
                    WHERE u.nickname = ?
                    ''', (nickname,))
    user_id = cursor.fetchall()[0][0]

    conn.close()  # close the connection
    return int(user_id)


def parse_loaded_users_info(PATH_TO_DB: str, PATH_TO_READ: str, PATH_TO_SAVE: str, PATH_TO_DEL: str):
    files = os.listdir(PATH_TO_READ)
    counter_update = 0
    counter_add = 0
    for user_file in files:
        if user_file == '.DS_Store':
            continue

        f = open(PATH_TO_READ + '/' + user_file, 'r')
        ratings = BeautifulSoup(f.read(), 'xml')
        nickname = user_file.replace('.xml', '')

        try:
            total_ratings = int(ratings.find('items').get('totalitems'))
        except:
            continue

        if total_ratings > 0:
            items = ratings.find_all('item')
            user_id = get_user_id(PATH_TO_DB=PATH_TO_DB, nickname=nickname)
            for item in items:
                boardgame_id = int(item.get('objectid'))

                rating = float(item.find('stats').find('rating').get('value'))
                num_of_plays = int(item.find('numplays').text)
                try:
                    comment = item.find('comment').text
                    comment_id = get_comment_id(PATH_TO_DB=PATH_TO_DB, comment=comment)
                except:
                    comment_id = None
                status = item.find('status')

                last_modified = status.get('lastmodified')[:10]
                own = int(status.get('own'))
                prevowned = int(status.get('prevowned'))
                for_trade = int(status.get('fortrade'))
                want = int(status.get('want'))
                want_to_play = int(status.get('wanttoplay'))
                want_to_buy = int(status.get('wanttobuy'))
                wishlist = int(status.get('wishlist'))
                preordered = int(status.get('preordered'))

                conn = sqlite3.connect(PATH_TO_DB)
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT rating_id
                    FROM ratings
                    WHERE (user_id = ?) AND (boardgame_id = ?) AND (rating = ?) AND (last_modified = ?)
                    """,
                               (user_id, boardgame_id, rating, last_modified))
                rating_info = cursor.fetchall()
                conn.close()

                if rating_info:
                    rating_id = int(rating_info[0][0])

                    conn = sqlite3.connect(PATH_TO_DB)
                    cursor = conn.cursor()
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
                    conn.close()
                    counter_update += 1

                else:
                    conn = sqlite3.connect(PATH_TO_DB)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO ratings (user_id, boardgame_id, rating, 
                        num_of_plays, comment_id, own, prevowned, for_trade, 
                        want, want_to_play, want_to_buy, wishlist, preordered, 
                        last_modified) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                                   (user_id, boardgame_id, rating, num_of_plays,
                                    comment_id, own, prevowned, for_trade, want,
                                    want_to_play, want_to_buy, wishlist, preordered,
                                    last_modified))
                    conn.commit()
                    conn.close()
                    counter_add += 1

            os.replace(PATH_TO_READ + '/' + user_file,  # move file from buffer dir
                       PATH_TO_SAVE + '/' + user_file)  # to saving dir

        else:
            os.replace(PATH_TO_READ + '/' + user_file,  # move file from buffer dir
                       PATH_TO_DEL + '/' + user_file)  # to temporary dir

        # Save checking date into db
        last_check = datetime.strptime(str(ratings.find('items').get('pubdate')),
                                       '%a, %d %b %Y %H:%M:%S %z').isoformat()[:10]

        conn = sqlite3.connect(PATH_TO_DB)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users 
            SET last_check = ? 
            WHERE nickname = ?
            """,
                       (last_check, nickname))
        conn.commit()
        conn.close()
    print(f'Updated: {counter_update} ratings, added: {counter_add} ratings')
