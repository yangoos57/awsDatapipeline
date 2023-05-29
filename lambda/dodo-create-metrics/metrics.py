from sklearn.metrics import *
import matplotlib.pyplot as plt
import datapane as dp

plt.switch_backend("Agg")


def create_metrics(y_true, y_pred, average="macro"):

    # create confusion_matrix plots
    num = confusion_matrix(y_true=y_true, y_pred=y_pred, labels=["High", "Moderate", "Low"])
    per = confusion_matrix(
        y_true=y_true, y_pred=y_pred, normalize="pred", labels=["High", "Moderate", "Low"]
    )

    num_con_mat = ConfusionMatrixDisplay(
        confusion_matrix=num,
        display_labels=["High", "Moderate", "Low"],
    )
    per_con_mat = ConfusionMatrixDisplay(
        confusion_matrix=per,
        display_labels=["High", "Moderate", "Low"],
    )

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(7, 7))

    num_con_mat.plot(cmap=plt.cm.binary, ax=axes[0], colorbar=False)
    per_con_mat.plot(cmap=plt.cm.Blues, ax=axes[1], colorbar=False)

    axes[0].set_title("Counts")
    axes[1].set_title("Rate")
    plt.tight_layout()
    plt.close()

    return {
        "plot": fig,
        "precision": precision_score(
            y_true,
            y_pred,
            average=average,
        )
        * 100,
        "recall": recall_score(y_true, y_pred, average=average) * 100,
        "f1_score": f1_score(y_true, y_pred, average=average) * 100,
        "f1_score(micro)": f1_score(y_true, y_pred, average="micro") * 100,
    }


def update_dashboard(df):

    # metrics of origin model
    origin = {
        "precision": 90.17740429505136,
        "recall": 84.1452089489236,
        "f1_score": 86.57407407407408,
        "f1_score(micro)": 90.87136929460581,
    }

    df.columns = ["True", "Prediction"]
    y_true = df.values.T[0]
    y_pred = df.values.T[1]

    # new metrics
    new = create_metrics(y_true, y_pred)

    # Compare new metrics with origin metrics
    num_list = []
    for key in ["precision", "recall", "f1_score", "f1_score(micro)"]:
        val = new[key] - origin[key]
        if val >= 0:
            change = True
        else:
            change = False

        num_list.append((str(round(new[key], 1)) + "%", str(abs(round(val, 1))) + "%", change))

    # create a dashboard
    app = dp.App(
        "# Metrics Dashboard : Random Forest Classifier",
        "#### 모델의 초기 성능과 최근 200회의 예측 성능을 비교하는 지표입니다.",
        "#### 모델 초기 지표를 확인하고 싶은 경우 아래 Plot의 Origin 항목을 눌러주세요.",
        dp.Group(
            dp.BigNumber(
                heading="Precision",
                value=num_list[0][0],
                change=num_list[0][1],
                is_upward_change=num_list[0][2],
            ),
            dp.BigNumber(
                heading="Recall",
                value=num_list[1][0],
                change=num_list[1][1],
                is_upward_change=num_list[1][2],
            ),
            dp.BigNumber(
                heading="F1-score",
                value=num_list[2][0],
                change=num_list[2][1],
                is_upward_change=num_list[2][2],
            ),
            dp.BigNumber(
                heading="F1-score(micro)",
                value=num_list[3][0],
                change=num_list[3][1],
                is_upward_change=num_list[3][2],
            ),
            columns=4,
        ),
        dp.Group(
            dp.Select(
                blocks=[
                    dp.Plot(new["plot"], label="New"),
                    dp.Media(
                        file="static/metrics/img/origin_con_mat.png", name="Image1", label="Origin"
                    ),
                ]
            ),
            dp.DataTable(df),
            columns=2,
        ),
    )

    # update html file
    app.save(path="static/metrics/model_metrics.html")

    return None
