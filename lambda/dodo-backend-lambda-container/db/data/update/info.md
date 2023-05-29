### 데이터 정보(update 폴더)

##### main.py 실행 결과를 저장하는 폴더

- w2v : word2vec 용 데이터

- book_info.parquet : pd.DataFrame

  - 검색 결과 제공에 활용될 장서 데이터
  - isbn13, bookname, authors, publisher, class_no, reg_date, bookImageURL

- book_info.parquet : pd.DataFrame

  - 검색 결과 제공에 활용될 장서 데이터
  - isbn13, bookname, authors, publisher, class_no, reg_date, bookImageURL

- extracted_keywords.parquet : pd.DataFrame

  - 도서 별 키워드 추출 결과
  - isbn13, keyword

- lib_book.parquet : pd.DataFrame

  - 도서관 별 보유 장서 리스트
  - isbn13, lib_code

- data for search : List

  - 자료 검색용 데이터
  - isbn, keyword
