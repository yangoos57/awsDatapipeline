from random import randint
from typing import List, Dict, Union
import pandas as pd
import requests
import aiohttp
import asyncio

# env
api_url = "https://api.yangoos.me"


def _generate_book_item_indices(v) -> List:
    """
    추천 도서 중 임의의 도서를 선택하기 위한 매서드입니다.
    해당 함수는 도서 리스트에 대한 Indices를 제공하는데 사용됩니다.
    """
    random_counts = randint(0, int(v * 0.25) + 1)
    return [randint(0, v - 1) for _ in range(random_counts)]


def _generate_user_id() -> str:
    """
    임의의 user_id를 생성합니다.
    user_choice table 용도로 사용됩니다.
    """
    return f"user-{randint(1000,1999)}"


def generate_keywords(min=1, max=5) -> List:
    """
    임의의 도서 키워드를 생성합니다.
    eng_han.csv에 기반해 랜덤으로 1 ~ 5개 단어를 선택합니다.
    """
    df = pd.read_csv("eng_han.csv")
    x = df["eng"].dropna().tolist()
    y = df["kor"].tolist()
    keywords_list = x + y
    random_counts = randint(min, max)
    indices = [randint(0, len(keywords_list) - 1) for _ in range(random_counts)]
    return [keywords_list[i] for i in indices]


def generate_lib(min=1, max=3) -> List:
    """
    임의로 검색 할 도서관을 선정합니다.
    1 ~ 3개 도서관을 선정합니다.
    """
    lib_name = [
        "강남",
        "강동",
        "강서",
        "개포",
        "고덕",
        "고척",
        "구로",
        "남산",
        "노원",
        "도봉",
        "동대문",
        "동작",
        "마포",
        "서대문",
        "송파",
        "양천",
        "영등포",
        "용산",
        "정독",
        "종로",
    ]
    random_counts = randint(min, max)
    indices = [randint(0, len(lib_name) - 1) for _ in range(random_counts)]
    return list(set([lib_name[i] for i in indices]))


async def _requests_book_items(session, search_payload) -> Dict:
    """
    비동기로 수행되는 도서 검색 API 입니다.
    임의의 사용자 검색을 만들기 위한 용도로 사용됩니다.
    """
    async with session.post(api_url + "/predict", json=search_payload) as response:
        return await response.json()


async def _record_user_choice(session, user_id, query_id, isbn13) -> requests:
    """
    비동기로 수행되는 user-choice API 입니다.
    user-choice는 키워드 검색을 통해 추천받은 도서 중 사용자가 어떠한 도서를 선택했는지를 저장하는 용도입니다.
    """
    payload = {"user_id": user_id, "query_id": query_id, "isbn13": isbn13}
    async with session.post(api_url + "/user-choice", json=payload) as response:
        return await response.json()


async def generate_item(user_search: List, selected_lib: List):
    """
    사용자가 키워드를 기반으로 도서를 제공받았고, 그 중 어떠한 도서를 선택했는지에 대한 정보를 생성하는 함수입니다.

    """
    async with aiohttp.ClientSession(trust_env=True) as session:
        # 사용자가 찾고자 하는 키워드와 도서관 정보를 기반으로 도서 검색 API을 통해 추천한다.
        # ex) '파이썬', '구로' => 도서 검색 API => 도서 50권 추천
        search_payload = {"user_search": user_search, "selected_lib": selected_lib}
        result_book_list = await _requests_book_items(
            session=session, search_payload=search_payload
        )
        query_id = result_book_list["query_id"]
        book_list = list(map(lambda x: x["isbn13"], result_book_list["result"]))

        # 추천받은 도서 중 사용자가 어떤 도서를 클릭했는지 user-choice API를 통해 저장한다.
        len_books = len(book_list)
        if len_books > 0:
            indicies = _generate_book_item_indices(len_books)
            user_id = _generate_user_id()
            for idx in indicies:
                await _record_user_choice(
                    session=session, user_id=user_id, query_id=query_id, isbn13=book_list[idx]
                )

        return {"search_payload": search_payload, "num of books": len_books, "query_id": query_id}


######## V1
# def _record_user_choice(user_id, query_id, isbn13) -> requests:
#     payload = {"user_id": user_id, "query_id": query_id, "isbn13": isbn13}
#     return requests.post(api_url + "/user-choice", json=payload)


# def _requests_book_items(search_payload) -> Dict:
#     req_for_book_items = requests.post(api_url + "/predict", json=search_payload)
#     return req_for_book_items.json()


# async def generate_item(user_search: List, selected_lib: List):
#     """
#     사용자가 키워드를 기반으로 도서를 제공받았고,
#     그 중 어떠한 도서를 선택했는지에 대한 정보를 생성하는 함수입니다.
#     """
#     # 사용자가 찾고자 하는 키워드와 도서관 정보를 기반으로 도서 검색 API을 통해 추천한다.
#     # ex) '파이썬', '구로' => 도서 검색 API => 도서 50권 추천
#     search_payload = {"user_search": user_search, "selected_lib": selected_lib}
#     result_book_list = _requests_book_items(search_payload=search_payload)
#     query_id = result_book_list["query_id"]
#     book_list = list(map(lambda x: x["isbn13"], result_book_list["result"]))

#     # 추천받은 도서 중 사용자가 어떤 도서를 클릭했는지 user-choice API를 통해 저장한다.
#     len_books = len(book_list)
#     if len_books > 0:
#         indicies = _generate_book_item_indices(len_books)
#         user_id = _generate_user_id()
#         for idx in indicies:
#             _record_user_choice(user_id=user_id, query_id=query_id, isbn13=book_list[idx])

#     return {
#         "search_payload": search_payload,
#         "num of books": len_books,
#         "query_id": query_id,
#     }

################################ sync Functions

# def _record_user_choice(user_id, query_id, isbn13) -> requests:
#     payload = {"user_id": user_id, "query_id": query_id, "isbn13": isbn13}
#     return requests.post(api_url + "/user-choice", json=payload)

# def _requests_book_items(book_payload) -> Dict:
#     req_for_book_items = requests.post(api_url + "/predict", json=book_payload)
#     return req_for_book_items.json()

# def generate_item(user_search: List, selected_lib: List):
#     book_payload = {"user_search": user_search, "selected_lib": selected_lib}
#     result_book_list = _requests_book_items(book_payload)

#     query_id = result_book_list["query_id"]
#     book_list = list(map(lambda x: x["isbn13"], result_book_list["result"]))

#     len_books = len(book_list)
#     if len_books > 0:
#         indicies = _generate_book_item_indices(len_books)
#         user_id = _generate_user_id()
#         for idx in indicies:
#             _record_user_choice(user_id=user_id, query_id=query_id, isbn13=book_list[idx])

#     return {"query_id": query_id}
