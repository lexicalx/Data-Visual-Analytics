# def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list=None) -> list:
#%%
from urllib.request import urlopen
import json

api_key='40936685d01995edbdfc7c9ba1474e47'

person_id = 287
vote_avg_threshold = 7

url = "https://api.themoviedb.org/3"
my_key = f'?api_key={api_key}'
query=f'/person/{person_id}/movie_credits'
with urlopen(url+query+my_key) as response:
    body = response.read()
response = json.loads(body)
movies = response['cast']
if vote_avg_threshold is not None:
    movies = [m for m in movies if m['vote_average'] >= vote_avg_threshold]
keep_keys = ['id','title','vote_average']
for i,m in enumerate(movies):
    movies[i] = {key: m[key] for key in keep_keys}


# return cast #* the instructions are unclear, are we supposed to drop the other fields? the ... is ... dumb

# def get_movie_credits_for_person(self, person_id:str, vote_avg_threshold:float=None)->list:
#         """
#         Using the TMDb API, get the movie credits for a person serving in a cast role
#         documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

#         :param string person_id: the id of a person
#         :param vote_avg_threshold: optional parameter to return the movie credit if it is >=
#             the specified threshold.
#             e.g., if the vote_avg_threshold is 5.0, then only return credits with a vote_avg >= 5.0
#         :rtype: list
#             return a list of dicts, one dict per movie credit with the following structure:
#                 [{'id': '97909' # the id of the movie credit
#                 'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
#                 'vote_avg': 5.0 # the float value of the vote average value for the credit}, ... ]
#         """
#         return NotImplemented

#%%
# def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list=None) -> list:
#     """
#     Get the movie cast for a given movie id, with optional parameters to exclude an cast member
#     from being returned and/or to limit the number of returned cast members
#     documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

#     :param string movie_id: a movie_id
#     :param list exclude_ids: a list of ints containing ids (not cast_ids) of cast members  that should be excluded from the returned result
#         e.g., if exclude_ids are [353, 455] then exclude these from any result.
#     :param integer limit: maximum number of returned cast members by their 'order' attribute
#         e.g., limit=5 will attempt to return the 5 cast members having 'order' attribute values between 0-4
#         If after excluding, there are fewer cast members than the specified limit, then return the remaining members (excluding the ones whose order values are outside the limit range). 
#         If cast members with 'order' attribute in the specified limit range have been excluded, do not include more cast members to reach the limit.
#         If after excluding, the limit is not specified, then return all remaining cast members."
#         e.g., if limit=5 and the actor whose id corresponds to cast member with order=1 is to be excluded,
#         return cast members with order values [0, 2, 3, 4], not [0, 2, 3, 4, 5]
#     :rtype: list
#         return a list of dicts, one dict per cast member with the following structure:
#             [{'id': '97909' # the id of the cast member
#             'character': 'John Doe' # the name of the character played
#             'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, ... ]
#             Note that this is an example of the structure of the list and some of the fields returned by the API.
#             The result of the API call will include many more fields for each cast member.
#     """
#     url = "https://api.themoviedb.org/3"
#     my_key = f'?api_key={self.api_key}'
#     query=f'/movie/{movie_id}/credits'
#     with urlopen(url+query+my_key) as response:
#         body = response.read()
#     response = json.loads(body)
#     cast = response['cast']
#     if limit is not None:
#         cast = cast[:limit]
#     if exclude_ids is not None:
#         for i,c in enumerate(cast):
#             if c['id'] in exclude_ids:
#                 cast=cast[:i] + cast[i+1:]
#     return cast