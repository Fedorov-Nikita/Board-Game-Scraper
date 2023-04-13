import sqlite3
from datetime import date

import numpy as np

from scr.utils import get_user_profile


def add_user_into_db(nickname: str, PATH_TO_DB: str):
    """
	Function check user nickname in db, and addd info about this user if needed
	==========
	Parameters:

	nickname: str - user nickname, his/her profile will be checked on boardgamegeek.com
	PATH_TO_DB: str - path to local bg_database (sqlite3)
	"""
    conn = sqlite3.connect(PATH_TO_DB)

    # create a cursor object
    cursor = conn.cursor()
    commit_flag = False

    # checking nickname in database
    cursor.execute('''
					SELECT user_id
					FROM users 
					WHERE nickname = ?
					''', (nickname,))
    if not cursor.fetchall():
        flag = True
    else:
        flag = False

    if flag:
        try:
            user_dict = get_user_profile(nickname)

            # get country code
            country_id = np.nan
            if user_dict['country'] != '':
                cursor.execute('''
                            SELECT country_id
                            FROM countries 
                            WHERE country = ?
                            ''', (user_dict['country'],))
                country_id = cursor.fetchall()
                if country_id:
                    country_id = country_id[0][0]
                else:
                    print('### No country in DB: ', user_dict['country'])

            website_id = np.nan
            if user_dict['website'] != '':
                cursor.execute('''
                                SELECT link_id
                                FROM links 
                                WHERE link = ?
                                ''', (user_dict['website'],))
                website_id = cursor.fetchall()
                if not website_id:
                    cursor.execute('''
                        INSERT INTO links (link) VALUES (?)
                        ''', (user_dict['website'],))
                    print('Add link to db -> ', user_dict['website'])
                    cursor.execute('''
                                SELECT link_id
                                FROM links 
                                WHERE link = ?
                                ''', (user_dict['website'],))
                    website_id = cursor.fetchall()[0][0]

            last_check = date.today()
            cursor.execute('''
                INSERT INTO users (nickname, country_id, website_id, registration_date, last_profile_update, last_login, last_check) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                nickname, country_id, website_id, user_dict['registration_date'], user_dict['last_profile_update'],
                user_dict['last_login'], last_check))
            commit_flag = True
        except:
            print(f'### Check {nickname} manually')
    if commit_flag:
        conn.commit()  # commit the changes
    conn.close()  # close the connection
