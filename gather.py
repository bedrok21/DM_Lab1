import os
import praw
import pandas as pd
import yaml
from dotenv import load_dotenv


load_dotenv()


REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
DATA_DIR = '/mnt/d/knu/4cource/2/dm/lab1/data/3/'


class RedditDataCollector:
    def __init__(self, config):
        with open(config, 'r') as file:
            self.config = yaml.safe_load(file)

        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )

    def load_posts(self):
        topic_subreddits = self.config.get('topic_subreddits', {'': self.config.get('subreddits', {})})
        for topic, subreddits in topic_subreddits.items():
            for subreddit in subreddits:
                self.fetch_subreddit_posts(topic, subreddit, self.config['posts_num'])

    def fetch_subreddit_posts(self, topic, subreddit_name, total_count=5000, chunk_size=25):
        subreddit = self.reddit.subreddit(subreddit_name)
        posts = []

        filename = f'reddit_{topic}_{subreddit_name}_posts.csv'
        path = f'{DATA_DIR}{filename}'

        header = not os.path.isfile(path)

        after = None
        collected = 0

        while collected < total_count:
            batch = subreddit.new(limit=min(1000, total_count - collected), params={'after': after})
            batch = list(batch)

            if not batch:
                print("No more posts to fetch.")
                break

            for index, post in enumerate(batch):
                posts.append(self.parse_post_to_list(post, topic))
                collected += 1

                if (collected % chunk_size == 0) or (collected >= total_count):
                    df = pd.DataFrame(posts, columns=self.get_columns())
                    df.to_csv(path, mode='a', header=header, index=False)
                    header = False
                    print(f'Appended {len(posts)} posts to {filename} (Total: {collected})')
                    posts = []

            after = batch[-1].name

        if posts:
            df = pd.DataFrame(posts, columns=self.get_columns())
            df.to_csv(path, mode='a', header=header, index=False)
            print(f'Appended final {len(posts)} posts to {filename} (Total: {collected})')

    @staticmethod
    def get_columns():
        return ['title', 'text', 'topic', 'subreddit', 'author', 'link', 'score', 'upvote_ratio', 'num_comments', 'created']

    @staticmethod
    def parse_post_to_list(post, topic):
        return [
            post.title,
            post.selftext,
            topic,
            post.subreddit.display_name,
            post.author.name if post.author else 'unknown',
            post.permalink,
            post.score,
            post.upvote_ratio,
            post.num_comments,
            post.created_utc,
        ]

    @staticmethod
    def parse_post_to_dict(post, topic):
        return {
            'title': post.title,
            'text': post.selftext,
            'topic': topic,
            'subreddit': post.subreddit.display_name,
            'author': post.author.name if post.author else 'unknown',
            'link': post.permalink,
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created': post.created_utc
        }


if __name__ == '__main__':
    rdc = RedditDataCollector('configs/gather1.yaml')
    rdc.load_posts()
