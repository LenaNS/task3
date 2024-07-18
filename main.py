from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Инициализация SparkSession
# spark = SparkSession.builder.appName("ProductCategory").getOrCreate()
# spark = SparkSession.builder.master("local[*]").appName('ProductCategory').getOrCreate()
spark = SparkSession.Builder().appName('ProductCategory').getOrCreate()

print(spark.version)
# Пример данных
products_data = [
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C"),
    (4, "Продукт D")
]

categories_data = [
    (1, "Категория 1"),
    (2, "Категория 2")
]

product_categories_data = [
    (1, 1),
    (1, 2),
    (2, 1),
    # Продукт C не имеет категорий
]

# Создание датафреймов

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_categories_df = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

# Метод для получения пар «Имя продукта – Имя категории» и продуктов без категорий
def get_product_category_pairs_and_orphan_products(products_df: DataFrame, product_categories_df: DataFrame):
    # Получаем пары «Имя продукта – Имя категории»
    product_category_pairs = products_df.join(
        product_categories_df,
        "product_id",
        "left"
    ).join(
        categories_df,
        "category_id",
        "left"
    ).select(
        "product_name",
        "category_name"
    )

    # Продукты без категорий
    orphan_products = products_df.join(
        product_categories_df,
        "product_id",
        "left_anti"
    ).select("product_name")

    return product_category_pairs, orphan_products

# Вызов метода
product_category_pairs, orphan_products = get_product_category_pairs_and_orphan_products(products_df, product_categories_df)

# Показать результат
print("Пары «Имя продукта – Имя категории»: ")
product_category_pairs.show()

