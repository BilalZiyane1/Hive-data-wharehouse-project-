
from datetime import datetime

import mlflow
import mlflow.spark

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler
)

from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
    LinearSVC
)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# --------------------------------------------------
# Configuration
# --------------------------------------------------

MLFLOW_TRACKING_URI = "http://mlflow:5055"

EXPERIMENT_NAME = "iris-sparkml-model-comparison"

MODEL_PATH = "/tmp/iris_best_model"


def train_compare_select():

    # --------------------------------------------------
    # 1. Configure MLflow
    # --------------------------------------------------

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # --------------------------------------------------
    # 2. Create Spark Session
    # --------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("Iris-SparkML-Model-Comparison")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------
    # 3. Load Iris dataset
    # --------------------------------------------------

    from sklearn.datasets import load_iris

    iris = load_iris()

    rows = []

    for features, label in zip(
        iris.data.tolist(),
        iris.target.tolist()
    ):
        rows.append((
            float(features[0]),
            float(features[1]),
            float(features[2]),
            float(features[3]),
            float(label)
        ))

    df = spark.createDataFrame(
        rows,
        [
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "label"
        ]
    )

    # --------------------------------------------------
    # 4. Split dataset
    # --------------------------------------------------

    train_full, test_df = df.randomSplit(
        [0.80, 0.20],
        seed=42
    )

    train_df, val_df = train_full.randomSplit(
        [0.75, 0.25],
        seed=42
    )

    # --------------------------------------------------
    # 5. Feature engineering
    # --------------------------------------------------

    assembler = VectorAssembler(
        inputCols=[
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width"
        ],
        outputCol="raw_features"
    )

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    # --------------------------------------------------
    # 6. Define candidate models
    # --------------------------------------------------

    models = {

        "LogisticRegression": LogisticRegression(
            maxIter=1000,
            featuresCol="features",
            labelCol="label"
        ),

        "DecisionTree": DecisionTreeClassifier(
            maxDepth=5,
            seed=42
        ),

        "RandomForest": RandomForestClassifier(
            numTrees=100,
            seed=42
        ),

        "GradientBoosting": GBTClassifier(
            maxIter=100,
            maxDepth=5,
            seed=42
        ),

        "LinearSVM": LinearSVC(
            maxIter=1000,
            featuresCol="features",
            labelCol="label"
        )
    }

    # --------------------------------------------------
    # 7. Define evaluators
    # --------------------------------------------------

    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        ),

        "precision": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision"
        ),

        "recall": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall"
        ),

        "f1": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1"
        )
    }

    results = []

    best_f1 = -1.0
    best_model_name = None
    best_run_id = None

    # --------------------------------------------------
    # 8. Train and evaluate candidates
    # --------------------------------------------------

    for model_name, classifier in models.items():

        print(f"\nTraining model: {model_name}")

        pipeline = Pipeline(
            stages=[
                assembler,
                scaler,
                classifier
            ]
        )

        with mlflow.start_run(
            run_name=model_name
        ) as run:

            model = pipeline.fit(train_df)

            predictions = model.transform(val_df)

            metrics = {}

            for metric_name, evaluator in evaluators.items():

                metrics[metric_name] = evaluator.evaluate(
                    predictions
                )

            # Log parameters
            mlflow.log_param(
                "model_name",
                model_name
            )

            mlflow.log_param(
                "algorithm",
                classifier.__class__.__name__
            )

            # Log metrics
            mlflow.log_metrics({
                f"val_{key}": value
                for key, value in metrics.items()
            })

            # Log Spark model
            mlflow.spark.log_model(
                spark_model=model,
                name="model"
            )

            results.append({
                "model": model_name,
                "val_accuracy": metrics["accuracy"],
                "val_precision": metrics["precision"],
                "val_recall": metrics["recall"],
                "val_f1": metrics["f1"],
                "run_id": run.info.run_id
            })

            print(
                f"{model_name} | "
                f"Accuracy={metrics['accuracy']:.4f} | "
                f"F1={metrics['f1']:.4f}"
            )

            # Select best model
            if metrics["f1"] > best_f1:

                best_f1 = metrics["f1"]
                best_model_name = model_name
                best_run_id = run.info.run_id

    # --------------------------------------------------
    # 9. Display model comparison
    # --------------------------------------------------

    print("\nMODEL COMPARISON")

    for result in sorted(
        results,
        key=lambda x: x["val_f1"],
        reverse=True
    ):

        print(
            f"{result['model']}: "
            f"F1={result['val_f1']:.4f}"
        )

    if best_run_id is None:
        raise RuntimeError(
            "No model was trained successfully."
        )

    print("\nBEST MODEL")

    print("Name:", best_model_name)
    print("Validation F1:", best_f1)
    print("MLflow run ID:", best_run_id)

    # --------------------------------------------------
    # 10. Load best model from MLflow
    # --------------------------------------------------

    best_model = mlflow.spark.load_model(
        f"runs:/{best_run_id}/model"
    )

    # --------------------------------------------------
    # 11. Evaluate best model on test dataset
    # --------------------------------------------------

    test_predictions = best_model.transform(test_df)

    test_metrics = {}

    for metric_name, evaluator in evaluators.items():

        test_metrics[metric_name] = evaluator.evaluate(
            test_predictions
        )

    # --------------------------------------------------
    # 12. Record final test evaluation
    # --------------------------------------------------

    with mlflow.start_run(
        run_name="best_model_test_evaluation"
    ):

        mlflow.log_params({
            "selected_model": best_model_name,
            "selected_run_id": best_run_id
        })

        mlflow.log_metrics({
            f"test_{key}": value
            for key, value in test_metrics.items()
        })

    print("\nFINAL TEST RESULTS")

    print("Model:", best_model_name)

    for metric_name, value in test_metrics.items():

        print(
            f"Test {metric_name}: {value:.4f}"
        )

    print("\nSpark ML model comparison completed.")

    spark.stop()


# --------------------------------------------------
# 13. Airflow DAG
# --------------------------------------------------

with DAG(
    dag_id="sparkml_model_comparison",

    description=(
        "Train, compare and select the best "
        "Spark ML model using MLflow"
    ),

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "spark",
        "spark-ml",
        "mlflow",
        "model-selection"
    ]

) as dag:

    compare_models = PythonOperator(
        task_id="train_compare_select",
        python_callable=train_compare_select
    )
