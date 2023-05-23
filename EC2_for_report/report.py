import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib as mpl
import datapane as dp
import pandas as pd
import boto3
import pytz
import os

plt.style.use("seaborn-bright")
# plt.style.available
plt.rcParams["font.family"] = "Noto Sans KR"
mpl.rcParams["axes.unicode_minus"] = False

tz = pytz.timezone("Asia/Tokyo")
today = datetime.now(tz)
date_time = today.strftime("%Y-%m-%d")
year, month, day = date_time.split("-")


def generate_report(date_time: str = date_time):

    # options
    plt.switch_backend("Agg")
    plt.style.use("seaborn-bright")
    plt.rcParams["font.family"] = "Noto Sans KR"
    mpl.rcParams["axes.unicode_minus"] = False

    db_dir = f"data/report/{year}/{month}/{day}"

    # download parquet
    client = boto3.client("s3")

    bucket = "dodomoabucket"
    prefix = f"redshift/{year}/{month}/{day}"
    obj_dir_list = client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]

    obj_dir_list = sorted(obj_dir_list, key=lambda x: x["LastModified"], reverse=True)
    obj_dir_list = list(map(lambda x: x["Key"], obj_dir_list))

    obj_dir_list = obj_dir_list[:8]
    obj_name_list = [
        "count_keyword",
        "count_book",
        "count_lib",
        "failed_keyword_extract",
        "user_requests_failed",
        "user_selection_per_recom",
        "user_requests_success",
    ]
    # match obj_name and obj_dir
    obj_dict = dict()
    for obj_name in obj_name_list:
        for obj_dir in obj_dir_list:
            if obj_name in obj_dir:
                obj_dict[obj_name] = obj_dir

    # if dir does not exist, download db_files
    if _check_directory(db_dir):
        for name, obj_dir in obj_dict.items():
            bucket_name = "dodomoabucket"
            object_key = obj_dir
            local_file_path = f"{db_dir}/{name}.parquet"
            client.download_file(bucket_name, object_key, local_file_path)

    # load parquet
    data_dict = dict()

    for obj in obj_name_list:
        data_dict[obj] = pd.read_parquet(f"{db_dir}/{obj}.parquet")

    # dp_stats variables
    requests_success = data_dict["user_requests_success"].values[0][0]
    requests_fail = data_dict["user_requests_failed"].values[0][0]

    requests = requests_success + requests_fail
    fail_ratio = requests_fail / requests_success
    user_select_per_recom = data_dict["user_selection_per_recom"].values[0][0]

    # dp_group variables
    title_dict = {
        "count_keyword": "유저 검색 키워드",
        "count_book": "도서 검색 순위",
        "count_lib": "도서관 검색 순위",
        "failed_keyword_extract": "검색 실패 키워드",
    }

    stats_group = dp.Group(
        dp.BigNumber(heading="사용자 요청 건수", value=format(requests, ",") + " 건"),
        dp.BigNumber(heading="도서 검색 실패율", value=format(fail_ratio, ".3f") + "%"),
        dp.BigNumber(
            heading="평균 도서 선택 권수",
            value=format(user_select_per_recom, ".2f") + " 건",
        ),
        columns=3,
    )

    section_group = [
        _create_dp_group_of(data_dict["count_keyword"], title_dict["count_keyword"]),
        _create_dp_group_of(
            data_dict["count_book"], title_dict["count_book"], column_name=["bookname", "count"]
        ),
        _create_dp_group_of(data_dict["count_lib"], title_dict["count_lib"]),
        _create_dp_group_of(
            data_dict["failed_keyword_extract"], title_dict["failed_keyword_extract"]
        ),
    ]

    app = dp.App(
        f"# 일일 앱 사용 결과 ({date_time})",
        "### ",
        stats_group,
        dp.Group(dp.Select(blocks=section_group)),
    )

    _check_directory(f"report/{date_time}")
    app.save(path=f"report/{date_time}/report_{date_time}.html")

    # update html file

    # return client.upload_file(
    #     f"report/{date_time}/report_{date_time}.html",
    #     "dodomoabucket",
    #     f"report/{date_time}/report_{date_time}.html",
    # )
    return print("success")


def _create_stack_barh(
    name_1,
    width_1,
    name_2=None,
    width_2=None,
    width_size: float = 1.05,
    height=0.85,
    color_1="#FFFFFF",
    color_2="#2373B8",
    title_name=None,
):

    fig, ax = plt.subplots(figsize=(4, 3))

    # name set
    if name_2 is None:
        name_2 = name_1
    if width_2 is None:
        width_2 = [max(width_1) * width_size] * len(name_2)

    # create bar
    ax.barh(y=name_2, width=width_2, height=height, color=color_1)
    ax.barh(y=name_1, width=width_1, height=height, color=color_2)

    # add labels
    for i, (x_value, y_value) in enumerate(zip(name_1, width_1)):
        ax.text(
            s=x_value, x=min(width_1) * 0.01, y=i, color="w", verticalalignment="center", size=8
        )
        ax.text(
            s=y_value,
            x=max(width_2) * width_size,
            y=i,
            color="k",
            verticalalignment="center",
            size=10,
        )

    # turn axis off
    plt.axis("off")
    # ax.set_ylabel("")
    # ax.set_yticklabels([])
    # ax.set_xlabel("")
    # ax.set_xticklabels([])

    if title_name is not None:
        plt.title(title_name)
    plt.close()
    return fig


def _create_dp_group_of(
    df,
    label: str,
    column_name: list = None,
):

    plot_data = df.head(10).sort_values("count")
    if column_name is None:
        column_name = df.columns
    name = plot_data[column_name[0]].map(lambda x: x if len(x) < 20 else x[:20] + "...").tolist()
    width = plot_data[column_name[1]].tolist()
    return dp.Group(
        dp.Plot(_create_stack_barh(name_1=name, width_1=width, title_name=label)),
        dp.DataTable(df.reset_index(drop=True).sort_values(by="count", ascending=False)),
        columns=2,
        label=label,
    )


def _check_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
        return True
    else:
        return False
