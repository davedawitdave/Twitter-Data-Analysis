import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        statuses_count= []  # count the statuses of
        for items in self.tweets_list:
            statuses_count.append(items['user']['status_count'])        #unfinished line
        return statuses_count

        
    def find_full_text(self)->list:
        text = []      #hold the clean text
        u_text=[]      #original text 
        for items in self_tweets_list:
            u_text.append(items['text'])           
            text.append(re.sub("^.*:", "", items['text']))        #unfinished line of code
        return text, u_text
  

    def find_sentiments(self, text)->list:
                polarity = [] # contains the polarity values from the sentiment analysis
        self.subjectivity = [] # contains the subjectivity values from the sentiment analysis
                
                """
        a function that extracts a polarity and a subjectivity from the list of tweet strings.
        returns a two lists of polarity and subjectivity scores.
        """
        for items in text:
            self.subjectivity.append(TextBlob(items).sentiment.subjectivity)
            polarity.append(TextBlob(items).sentiment.polarity)

        return polarity, self.subjectivity

    def find_created_time(self)->list:
        # a function that extracts when from created_at  variable and returns a list of date strings.
   
        created_at = []   # aholds a list of all created time and date
        for items in self.tweets_list:
            created_at.append(items.get('created_at', None))
        
        return created_at

    def find_source(self)->list:
        source = []      
                 """
        a function that extracts the source variable and returns a list of html hyperlink 
        reference strings.
        """
        for items in self.tweets_list:
            source.append(items['source'])
        return source 

    def find_screen_name(self)->list:
        # a function that extracts screen name from users.
        screen_name =[]   # list of screen names.

        for items in self.tweets_list:
            screen_name.append(items['user']['screen_name'])
        return screen_name

    def find_followers_count(self)->list:
        # a function for counting the number of followers for each user
        followers_count = []

        for items in self.tweets_list:
            followers_count.append(items['user']['followers_count'])
        return followers_count

    def find_friends_count(self)->list:
        # a function for counting the number of friends users have
        friends_count = []
        for items in self.tweets_list:
            friends_count.append(items['user'][friends_count])
        return friends_count

    def is_sensitive(self)->list:
        # a funciton returns the sensitivity of users tweet
        is_sensitive=[]
        try:
            is_sensitive = [x['possibly_sensitive'] for x in self.tweets_list]
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self)->list:
        # a function that extracts the number of favourties.

        favourite_count = [] # listing the number of favourites.
        for items in self.tweets_list:
            favourite_count.append(items['user']['favourites_count'])
        
        return favourite_count
    
    def find_retweet_count(self)->list:
        #function return the number of retweets
        retweet_count = []
        for items in self.tweets_list:
            retweet_count.append(items['user']['retweet_count'])
        return retweet_count

    def find_hashtags(self)->list:
        # a function that extracts the hashtags used in the tweet.
   
        hashtags =[]
        for items in self.tweets_list:
            hashtags.append(items.get('entities', {}).get('hashtags', None))        
        
        return hashtags

    def find_mentions(self)->list:
        # a function for finding mentions 
        mentions = []
        for items in self.tweets_list:
            mentions.append(items('entities', {}).get('mentions', None))

        return mentions

    def find_location(self)->list:
        location=[]
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""     #OK 
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("../data/global_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above