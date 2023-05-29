from gensim.models import keyedvectors
from sqlalchemy.orm import Session
from typing import *
import db.cruds as query
import numpy as np
import pandas as pd
import pickle


class BookSearcher:
    def __init__(self) -> None:
        """w2v 모델 및 도서 키워드 데이터 로드"""
        self.model = keyedvectors.load_word2vec_format("db/data/update/w2v")
        self.converter = dict(pd.read_csv("db/data/preprocess/eng_han.csv").values)
        self.data = self._read_pkl("db/data/update/data_for_search")
        self.isbn_array = self.data[0]
        self.book_keyword_array = self.data[1]

    def extract_recommand_book_isbn(
        self,
        user_search: List[str],
        book_isbn: np.array,
        book_keyword: np.array,
    ) -> Dict[str, int]:
        """사용자 검색 결과에 대한 도서 추천 결과를 isbn으로 반환"""
        # extract recommandation keywords
        try:
            recommand_keyword = self.model.most_similar(positive=user_search, topn=15)
        except KeyError as e:
            raise KeyError(f"{user_search} is not in vocab")

        np_recommand_keyword = np.array(list(map(lambda x: x[0], recommand_keyword)))

        # calculate recommandation points
        user_point = np.isin(book_keyword, np.array(user_search)).sum(axis=1)
        recommand_point = np.isin(book_keyword, np_recommand_keyword).sum(axis=1)
        total_point = (user_point * 3) + recommand_point

        top_k_idx = np.argsort(total_point)[::-1][:50]
        return dict(zip(book_isbn[top_k_idx], total_point[top_k_idx]))

    def create_book_recommandation_df(self, db: Session, data: Dict) -> pd.DataFrame:
        """
        추천 결과를 바탕으로 도서 데이터 추출
        columns = [publisher, isbn13, bookname, reg_date, class_no, authors, bookImageURL, lib_name]
        """

        # eng to kor
        def map_eng_to_kor(word: str) -> str:
            kor_word = self.converter.get(word)
            return kor_word if kor_word else word

        converted_data = list(map(lambda x: map_eng_to_kor(x.lower()), data["user_search"]))

        # collect books in selected_lib
        set_isbn_in_selected_lib = set(query.load_lib_isbn(db, data["selected_lib"]))
        BM = list(map(lambda x: True if x in set_isbn_in_selected_lib else False, self.isbn_array))
        book_isbn = self.isbn_array[BM]
        book_keyword = self.book_keyword_array[BM, :]

        # recommandation result
        isbn_dict = self.extract_recommand_book_isbn(converted_data, book_isbn, book_keyword)

        db_result = query.check_books_in_selected_lib(
            db, list(isbn_dict.keys()), data["selected_lib"]
        )
        # book list with libraries
        # 도서 : 도커와 쿠버네티스, 도서관 : [서대문, 강서]
        lib_book_df = (
            pd.DataFrame(db_result)
            .groupby(by="isbn13")
            .agg(list)
            .applymap(lambda x: " ".join(list(set(x))))
            .reset_index()
        )

        # load book_info
        db_result = query.load_book_info(db, lib_book_df.isbn13.tolist())
        book_info_df = pd.DataFrame(db_result)

        # merge result
        book_df = pd.merge(book_info_df, lib_book_df, on="isbn13").sort_values(
            by="isbn13", key=lambda x: x.map(isbn_dict), ascending=False
        )
        book_df["reg_date"] = book_df["reg_date"].astype(str)
        return book_df

    def _read_pkl(self, dir: str):
        with open(dir, "rb") as fp:
            data = pickle.load(fp)
            return data

    def _save_pkl(self, item, dir: str = "test"):
        # store list in binary file so 'wb' mode
        with open(f"{dir}", "wb") as fp:
            pickle.dump(item, fp)
            print("Done writing list into a binary file")
