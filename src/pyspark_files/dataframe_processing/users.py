import random
import logging
logger = logging.getLogger(__name__)

class Users:
    def __init__(self, spark_session, user_number):
        if isinstance(user_number, int) is True and 0 < user_number < 100000:
            self.__users_df = self.__generate_user_df(user_number, spark_session)
        else:
            logger.error("ERROR: Too many users, exiting!")
            self.__users_df = None


    def get_df(self):
        return self.__users_df


    def __generate_name(self):
        first_names = ('Mary', 'July', 'Andrea', 'Athena', 'Diana',
                       'Alexandra', 'John', 'Michael', 'Jeremiah', 'Bill')
        last_names = ('Dirk', 'Scott', 'Jensen', 'Miller', 'Killjoy',
                      'Jobs', 'Gates', 'Bezos', 'Musk', 'Titus')
        return random.choice(first_names)+" "+random.choice(last_names)

    def __generate_user_df(self, number, spark):
        ids = (i for i in range(1, number+1))
        names = (self.__generate_name() for i in range(0, number))
        return spark.createDataFrame(zip(ids, names), ["id", "name"])
