import json
import re

with open("elastic_data.json") as read_it:
    data = json.load(read_it)
    print(type(data))


def record_id_list():
    record_id = []
    for m in range(len(data["hits"]["hits"])):
        record_id.append(data["hits"]["hits"][m]["_id"])

    return record_id


def id_dupicate_count():
    id_list = record_id_list()
    id_list_duplicate = []
    for i in range(0, len(id_list)):
        for j in range(i + 1, len(id_list)):
            if id_list[i] == id_list[j]:
                id_list_duplicate.append(i)

    return id_list_duplicate


def scan_dataset(field_name):
    test_data = {}
    for m in range(len(data["hits"]["hits"])):
        record_id = data["hits"]["hits"][m]["_id"]
        field = data["hits"]["hits"][m]["_source"][field_name]
        test_data[record_id] = field
    return test_data


def check_email(record_id):
        id_email_test_fail = []
        id_email = scan_dataset(record_id)
        regex = '^[A-Za-z0-9]+[.-_]*[A-Za-z0-9]+@[A-Za-z0-9-]+[.][A-Z|a-z]{2,}+'
        for k in id_email:
            if re.search(regex, id_email[k]) is None:
                id_email_test_fail.append(k)

        return id_email_test_fail


def scan_id_products():
    test_data = {}
    for m in range(len(data["hits"]["hits"])):
        record_id = data["hits"]["hits"][m]["_id"]
        field = data["hits"]["hits"][m]["_source"]["products"]
        test_data[record_id] = field
    return test_data


def scan_numeric_product(prod_field):
    id_invalid_field_item = {}
    dict_list = scan_id_products()
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if current_item[prod_field] is None or ((isinstance(current_item[prod_field], float) is not True) and (
                    isinstance(current_item[prod_field], int) is not True)):
                invalid_field_item = {}
                invalid_field_item[current_item["product_id"]] = current_item[prod_field]
                id_invalid_field_item[k] = invalid_field_item

    return id_invalid_field_item


def scan_alphabetic_product(prod_field):
    id_invalid_field_item = {}
    dict_list = scan_id_products()
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if current_item[prod_field] is None or current_item[prod_field].isspace() or any(char.isdigit() for char in current_item[prod_field]):
                invalid_field_item = {}
                invalid_field_item[current_item["product_id"]] = current_item[prod_field]
                id_invalid_field_item[k] = invalid_field_item

    return id_invalid_field_item


def scan__id_product(prod_field):
    id_invalid_field_item = {}
    dict_list = scan_id_products()
    str1 = "sold_product_"
    order_id = scan_dataset("order_id")
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if current_item[prod_field] is None or current_item[prod_field].isspace() or (str1 not in current_item[prod_field] and str(current_item["product_id"]) not in current_item[prod_field] and str(order_id[k]) not in current_item[prod_field]):
                invalid_field_item = {}
                invalid_field_item[current_item["product_id"]] = current_item[prod_field]
                id_invalid_field_item[k] = invalid_field_item

    return id_invalid_field_item


def scan_alpha_num_product(prod_field):
    id_invalid_field_item = {}
    dict_list = scan_id_products()
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if current_item[prod_field] is None or current_item[prod_field].isspace():
                invalid_field_item = {}
                invalid_field_item[current_item["product_id"]] = current_item[prod_field]
                id_invalid_field_item[k] = invalid_field_item

    return id_invalid_field_item


def scan_date_product(prod_field):
    id_invalid_field_item = {}
    dict_list = scan_id_products()
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if re.search(date_pattern_regex(), current_item[prod_field]) is None:
                invalid_field_item = {}
                invalid_field_item[current_item["product_id"]] = current_item[prod_field]
                id_invalid_field_item[k] = invalid_field_item

    return id_invalid_field_item


def date_pattern_regex():
    date_pattern = r"^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-][01]\d:[0-5]\d)$"

    return date_pattern


def country_pattern_regex():
    country_pattern = "^([A-Z]{2,3})"
    return country_pattern


def check_date(record_id):
    id_order_date_test_fail = []
    id_order_date = scan_dataset(record_id)
    for k in id_order_date:
        if re.search(date_pattern_regex(), id_order_date[k]) is None:
            id_order_date_test_fail.append(k)
    return id_order_date_test_fail


def check_sting_float(record_id):
    id_string_float = scan_dataset(record_id)
    id_test_fail_list = []
    for k in id_string_float:
        if id_string_float[k] is None or isinstance(id_string_float[k], float) is not True:
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_string_alpha(record_id):
    id_string_alpha = scan_dataset(record_id)
    id_test_fail_list = []
    for k in id_string_alpha:
        if id_string_alpha[k] is None or id_string_alpha[k].isspace() or any(char.isdigit() for char in id_string_alpha[k]):
            id_test_fail_list.append(k)

    return id_test_fail_list


def check_list(record_id):
    id_test_fail_list = []
    id_test = scan_dataset(record_id)
    for k in id_test:
        if id_test[k] is None:
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_event(record_id):
    event = {"dataset": "sample_ecommerce"}
    id_test_fail_list = []
    id_check_event = scan_dataset(record_id)
    for k in id_check_event:
        if id_check_event[k] != event:
            id_test_fail_list.append(k)

    return id_test_fail_list


def check_country_iso(record_id):
    id_test_fail_list = []
    for k in record_id:
        if re.search(country_pattern_regex(), record_id[k]["country_iso_code"]) is None:
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_continent_name(record_id):
    id_test_fail_list = []
    continents = ["Africa", "Antarctica", "Asia", "Oceania", "Europe", "North America", "South America"]
    for k in record_id:
        if (record_id[k]["continent_name"]) not in continents:
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_country_location(record_id):
    id_test_fail_list = []
    for k in record_id:
        lon = record_id[k]["location"]["lon"]
        lat = record_id[k]["location"]["lat"]
        if (lon < -180 or lon > 180) or (lat < -90 or lat > 90):
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_gender(record_id):
    record_id_customer_gender = scan_dataset(record_id)
    gender = ["MALE", "FEMALE"]
    id_customer_gender_test_fail = []
    for k in record_id_customer_gender:
        if record_id_customer_gender[k] not in gender:
            id_customer_gender_test_fail.append(k)
    return id_customer_gender_test_fail


def check_string_int(record_id):
    id_test_fail_list = []
    id_int_string = scan_dataset(record_id)
    for k in id_int_string:
        if id_int_string[k] is None or isinstance(id_int_string[k], int) is not True:
            id_test_fail_list.append(k)
    return id_test_fail_list


def check_day_of_week(record_id):
    record_id_day_of_week = scan_dataset(record_id)
    day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    id_day_of_week_test_fail = []
    for k in record_id_day_of_week:
        if record_id_day_of_week[k] not in day_of_week:
            id_day_of_week_test_fail.append(k)
    return id_day_of_week_test_fail


def check_dat_of_week_i(record_id):
    record_id_day_of_week_i = scan_dataset(record_id)
    record_id_day_of_week = scan_dataset("day_of_week")
    day_of_week_current = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday",
                           6: "Sunday"}
    day_of_week_i = [0, 1, 2, 3, 4, 5, 6]
    id_day_of_week_i_test_fail = []
    for k in record_id_day_of_week_i:
        if record_id_day_of_week_i[k] not in day_of_week_i or (
                day_of_week_current[record_id_day_of_week_i[k]] != record_id_day_of_week[k]):
            id_day_of_week_i_test_fail.append(k)
    return id_day_of_week_i_test_fail


def get_sku(prod_field):
    id_test_fail_list = []
    sku_ext = scan_dataset("sku")
    dict_list = scan_id_products()
    for k in dict_list:
        item = dict_list[k]
        for j in range(len(item)):
            current_item = item[j]
            if current_item[prod_field] not in sku_ext[k]:
                id_test_fail_list.append(k)
    return id_test_fail_list


def report(report, test_name, file_name):
    report_data = {}
    report_data [test_name] = report
    with open(file_name, "w") as file:
        json.dump(report_data, file, indent=4)


def test_email():
    id_email_test_fail = check_email("email")
    invalid_email_count = len(id_email_test_fail)
    if invalid_email_count is not 0:
        report(id_email_test_fail, "test_email", "test_email_failed_record_ids.json")
    assert invalid_email_count == 0



def test_id_record_duplicate():
    id_record_duplicate_list = id_dupicate_count()
    id_record_duplicate_list_count = len(id_record_duplicate_list)
    if id_record_duplicate_list_count is not 0:
        report(id_record_duplicate_list, "test_id_record_duplicate", "test_id_record_duplicate_failed_record_ids.json")
    assert id_record_duplicate_list_count == 0


def test_category():
    id_category_test_fail = check_list("category")
    invalid_category_count = len(id_category_test_fail)
    if invalid_category_count is not 0:
        report(id_category_test_fail, "test_category", "test_category_failed_record_ids.json")
    assert invalid_category_count == 0


def test_currency():
    id_currency_test_fail = check_string_alpha("currency")
    invalid_currency_count = len(id_currency_test_fail)
    if invalid_currency_count is not 0:
        report(id_currency_test_fail, "test_currency", "test_currency_failed_record_ids.json")
    assert invalid_currency_count == 0


def test_customer_first_name():
    id_customer_first_name_test_fail = check_string_alpha("customer_first_name")
    invalid_customer_first_name_count = len(id_customer_first_name_test_fail)
    if invalid_customer_first_name_count is not 0:
        report(id_customer_first_name_test_fail, "test_customer_first_name", "test_customer_first_name_failed_record_ids.json")
    assert invalid_customer_first_name_count == 0


def test_customer_full_name():
    id_customer_full_name_test_fail = check_string_alpha("customer_full_name")
    invalid_customer_full_name_count = len(id_customer_full_name_test_fail)
    if invalid_customer_full_name_count is not 0:
        report(id_customer_full_name_test_fail, "test_customer_full_name",
           "test_customer_full_name_failed_record_ids.json")

    assert invalid_customer_full_name_count == 0


def test_customer_gender():
    id_customer_gender_test_fail = check_gender("customer_gender")
    invalid_customer_gender_count = len(id_customer_gender_test_fail)
    if invalid_customer_gender_count is not 0:
        report(id_customer_gender_test_fail, "test_customer_gender",
           "test_customer_gender_failed_record_ids.json")
    assert invalid_customer_gender_count == 0


def test_customer_id():
    id_customer_id_test_fail = check_string_int("customer_id")
    invalid_customer_id_count = len(id_customer_id_test_fail)
    if invalid_customer_id_count is not 0:
        report(id_customer_id_test_fail, "test_customer_id",
           "test_customer_id_failed_record_ids.json")
    assert invalid_customer_id_count == 0


def test_customer_last_name():
    id_customer_last_name_test_fail = check_string_alpha("customer_last_name")
    invalid_customer_last_name_count = len(id_customer_last_name_test_fail)
    if invalid_customer_last_name_count is not 0:
        report(id_customer_last_name_test_fail, "test_customer_last_name",
           "test_customer_last_name_failed_record_ids.json")
    assert invalid_customer_last_name_count == 0


def test_customer_phone():
    id_customer_phone_test_fail = check_string_int("customer_phone")
    invalid_customer_phone_count = len(id_customer_phone_test_fail)
    if invalid_customer_phone_count is not 0:
        report(id_customer_phone_test_fail, "test_customer_phone", "test_customer_phone_failed_record_ids.json")
    assert invalid_customer_phone_count == 0


def test_day_of_week():
    id_day_of_week_fail = check_day_of_week("day_of_week")
    invalid_day_of_week_count = len(id_day_of_week_fail)
    if invalid_day_of_week_count is not 0:
        report(id_day_of_week_fail, "test_day_of_week", "test_day_of_week_failed_record_ids.json")
    assert invalid_day_of_week_count == 0


def test_day_of_week_i():
    id_day_of_week_i_fail = check_dat_of_week_i("day_of_week_i")
    invalid_day_of_week_i_count = len(id_day_of_week_i_fail)
    if invalid_day_of_week_i_count is not 0:
        report(id_day_of_week_i_fail, "test_day_of_week_i", "test_day_of_week_i_failed_record_ids.json")
    assert invalid_day_of_week_i_count == 0


def test_manufacturer():
    id_manufacturer_test_fail = check_list("manufacturer")
    invalid_manufacturer_count = len(id_manufacturer_test_fail)
    if invalid_manufacturer_count is not 0:
        report(id_manufacturer_test_fail, "test_manufacturer", "test_manufacturer_failed_record_ids.json")
    assert invalid_manufacturer_count == 0


def test_order_date():
    invalid_order_date_test_fail = check_date("order_date")
    invalid_order_date_count = len(invalid_order_date_test_fail)
    if invalid_order_date_count is not 0:
        report(invalid_order_date_test_fail, "test_order_date", "test_order_date_failed_record_ids.json")
    assert invalid_order_date_count == 0


def test_order_id():
    id_order_id_test_fail = check_string_int("order_id")
    invalid_order_id_count = len(id_order_id_test_fail)
    if invalid_order_id_count is not 0:
        report(id_order_id_test_fail, "test_order_id", "test_order_id_failed_record_ids.json")
    assert invalid_order_id_count == 0


def test_products_base_price():
    id_invalid_products_base_price = scan_numeric_product("base_price")
    id_invalid_base_price_item_count = len(id_invalid_products_base_price)
    if id_invalid_base_price_item_count is not 0:
        report(id_invalid_products_base_price, "test_products_base_price", "test_products_base_price_id_failed_record_ids.json")
    assert id_invalid_base_price_item_count == 0


def test_products_discount_percentage():
    id_invalid_products_base_price = scan_numeric_product("discount_percentage")
    id_invalid_discount_percentage_item_count = len(id_invalid_products_base_price)
    if id_invalid_discount_percentage_item_count is not 0:
        report(id_invalid_products_base_price, "test_products_discount_percentage", "test_products_discount_percentage_id_failed_record_ids.json")
    assert id_invalid_discount_percentage_item_count == 0


def test_products_quantity():
    id_invalid_products_quantity = scan_numeric_product("quantity")
    id_invalid_quantity_item_count = len(id_invalid_products_quantity)
    if id_invalid_quantity_item_count is not 0:
        report(id_invalid_products_quantity, "test_products_quantity", "test_products_quantity_id_failed_record_ids.json")

    assert id_invalid_quantity_item_count == 0


def test_products_tax_amount():
    id_invalid_products_tax_amount = scan_numeric_product("tax_amount")
    id_invalid_tax_amount_item_count = len(id_invalid_products_tax_amount)
    if id_invalid_tax_amount_item_count is not 0:
        report(id_invalid_products_tax_amount, "test_products_tax_amount", "test_products_tax_amount_id_failed_record_ids.json")

    assert id_invalid_tax_amount_item_count == 0


def test_products_product_id():
    id_invalid_products_product_id = scan_numeric_product("product_id")
    id_invalid_product_id_item_count = len(id_invalid_products_product_id)
    if id_invalid_product_id_item_count is not 0:
        report(id_invalid_products_product_id, "test_products_product_id", "test_products_product_id_id_failed_record_ids.json")

    assert id_invalid_product_id_item_count == 0


def test_products_taxless_price():
    id_invalid_products_taxless_price = scan_numeric_product("taxless_price")
    id_invalid_product_taxless_price_count = len(id_invalid_products_taxless_price)
    if id_invalid_product_taxless_price_count is not 0:
        report(id_invalid_products_taxless_price, "test_products_taxless_price", "test_products_taxless_price_id_failed_record_ids.json")

    assert id_invalid_product_taxless_price_count == 0


def test_products_unit_discount_amount():
    id_invalid_products_unit_discount_amount = scan_numeric_product("unit_discount_amount")
    id_invalid_product_unit_discount_amount_count = len(id_invalid_products_unit_discount_amount)
    if id_invalid_product_unit_discount_amount_count is not 0:
        report(id_invalid_products_unit_discount_amount, "test_products_unit_discount_amount", "test_products_unit_discount_amount_id_failed_record_ids.json")

    assert id_invalid_product_unit_discount_amount_count == 0


def test_products_min_price():
    id_invalid_products_min_price = scan_numeric_product("min_price")
    id_invalid_product_min_price_count = len(id_invalid_products_min_price)
    if id_invalid_product_min_price_count is not 0:
        report(id_invalid_products_min_price, "test_products_min_price", "test_products_min_price_id_failed_record_ids.json")

    assert id_invalid_product_min_price_count == 0


def test_products_discount_amount():
    id_invalid_products_discount_amount = scan_numeric_product("discount_amount")
    id_invalid_product_discount_amount_count = len(id_invalid_products_discount_amount)
    if id_invalid_product_discount_amount_count is not 0:
        report(id_invalid_products_discount_amount, "test_products_discount_amount", "test_products_discount_amount_id_failed_record_ids.json")

    assert id_invalid_product_discount_amount_count == 0


def test_products_created_on():
    id_invalid_products_created_on = scan_date_product("created_on")
    id_invalid_product_created_on_count = len(id_invalid_products_created_on)
    if id_invalid_product_created_on_count is not 0:
        report(id_invalid_products_created_on, "test_products_created_on", "test_products_created_on_id_failed_record_ids.json")

    assert id_invalid_product_created_on_count == 0


def test_products_price():
    id_invalid_products_price = scan_numeric_product("price")
    id_invalid_product_price_count = len(id_invalid_products_price)
    if id_invalid_product_price_count is not 0:
        report(id_invalid_products_price, "test_products_price", "test_products_price_id_failed_record_ids.json")

    assert id_invalid_product_price_count == 0


def test_products_taxful_price():
    id_invalid_products_taxful_price = scan_numeric_product("taxful_price")
    id_invalid_product_taxful_price_count = len(id_invalid_products_taxful_price)
    if id_invalid_product_taxful_price_count is not 0:
        report(id_invalid_products_taxful_price, "test_products_taxful_price", "test_products_taxful_price_id_failed_record_ids.json")

    assert id_invalid_product_taxful_price_count == 0


def test_products_base_unit_price():
    id_invalid_products_base_unit_price = scan_numeric_product("base_unit_price")
    id_invalid_product_base_unit_price_count = len(id_invalid_products_base_unit_price)
    if id_invalid_product_base_unit_price_count is not 0:
        report(id_invalid_products_base_unit_price, "test_base_unit_price", "test_base_unit_price_id_failed_record_ids.json")

    assert id_invalid_product_base_unit_price_count == 0


def test_products_manufacturer():
    id_invalid_products_manufacturer = scan_alphabetic_product("manufacturer")
    id_invalid_product_manufacturer_count = len(id_invalid_products_manufacturer)
    if id_invalid_product_manufacturer_count is not 0:
        report(id_invalid_products_manufacturer, "test_products_manufacturer", "test_products_manufacturer_id_failed_record_ids.json")

    assert id_invalid_product_manufacturer_count == 0


def test_products_category():
    id_invalid_products_category = scan_alphabetic_product("category")
    id_invalid_product_category_count = len(id_invalid_products_category)
    if id_invalid_product_category_count is not 0:
        report(id_invalid_products_category, "test_products_category", "test_products_category_id_failed_record_ids.json")

    assert id_invalid_product_category_count == 0


def test_products_product_name():
    id_invalid_products_product_name = scan_alpha_num_product("product_name")
    id_invalid_product_name_count = len(id_invalid_products_product_name)
    if id_invalid_product_name_count is not 0:
        report(id_invalid_products_product_name, "test_products_product_name", "test_products_product_name_id_failed_record_ids.json")

    assert id_invalid_product_name_count == 0


def test_products__id():
    id_invalid_products__id = scan__id_product("_id")
    id_invalid_products__id_count = len(id_invalid_products__id)
    if id_invalid_products__id_count is not 0:
        report(id_invalid_products__id, "test_products__id", "test_products__id_id_failed_record_ids.json")

    assert id_invalid_products__id_count == 0


def test_products_sku():
    id_invalid_products_sku = scan_alpha_num_product("product_name")
    id_invalid_products_sku_count = len(id_invalid_products_sku)
    if id_invalid_products_sku_count is not 0:
        report(id_invalid_products_sku, "test_products_sku", "test_products_sku_id_failed_record_ids.json")

    assert id_invalid_products_sku_count == 0


def test_sku():
    id_sku_test_fail = get_sku("sku")
    invalid_sku_count = len(id_sku_test_fail)
    if invalid_sku_count is not 0:
        report(id_sku_test_fail, "test_sku", "test_sku_id_failed_record_ids.json")

    assert invalid_sku_count == 0


def test_taxful_total_price():
    id_taxful_total_price_test_fail = check_sting_float("taxful_total_price")
    invalid_taxful_total_price_count = len(id_taxful_total_price_test_fail)
    if invalid_taxful_total_price_count is not 0:
        report(id_taxful_total_price_test_fail, "test_taxful_total_price", "test_taxful_total_price_id_failed_record_ids.json")

    assert invalid_taxful_total_price_count == 0


def test_taxless_total_price():
    id_taxless_total_price_test_fail = check_sting_float("taxless_total_price")
    invalid_taxless_total_price_count = len(id_taxless_total_price_test_fail)
    if invalid_taxless_total_price_count is not 0:
        report(id_taxless_total_price_test_fail, "test_taxless_total_price", "test_taxless_total_price_id_failed_record_ids.json")

    assert invalid_taxless_total_price_count == 0


def test_total_quantity():
    id_order_total_quantity_test_fail = check_string_int("total_quantity")
    invalid_order_total_quantity_count = len(id_order_total_quantity_test_fail)
    if invalid_order_total_quantity_count is not 0:
        report(id_order_total_quantity_test_fail, "test_total_quantity", "test_total_quantity_id_failed_record_ids.json")

    assert invalid_order_total_quantity_count == 0


def test_total_unique_products():
    id_order_total_unique_products_test_fail = check_string_int("total_unique_products")
    invalid_order_total_unique_products_count = len(id_order_total_unique_products_test_fail)
    if invalid_order_total_unique_products_count is not 0:
        report(id_order_total_unique_products_test_fail, "test_total_unique_products", "test_total_unique_products_id_failed_record_ids.json")

    assert invalid_order_total_unique_products_count == 0


def test_type():
    id_type_test_fail = check_list("type")
    invalid_type_count = len(id_type_test_fail)
    if invalid_type_count is not 0:
        report(id_type_test_fail, "test_type", "test_type_id_failed_record_ids.json")

    assert invalid_type_count == 0


def test_user():
    id_user_test_fail = check_list("user")
    invalid_user_count = len(id_user_test_fail)
    if invalid_user_count is not 0:
        report(id_user_test_fail, "test_user", "test_user_id_failed_record_ids.json")

    assert invalid_user_count == 0


def test_event_check():
    id_event_test_fail = check_event("event")
    invalid_event_count = len(id_event_test_fail)
    if invalid_event_count is not 0:
        report(id_event_test_fail, "test_event_check", "test_event_check_id_failed_record_ids.json")

    assert invalid_event_count == 0


def test_geoip_country_iso_code():
    id_geoip_country_iso_codetest_fail = check_country_iso(scan_dataset("geoip"))
    invalid_geoip_country_iso_code_count = len(id_geoip_country_iso_codetest_fail)
    if invalid_geoip_country_iso_code_count is not 0:
        report(id_geoip_country_iso_codetest_fail, "test_geoip_country_iso_code", "test_geoip_country_iso_code_id_failed_record_ids.json")

    assert invalid_geoip_country_iso_code_count == 0


def test_geoip_location():
    id_geoip_location_fail = check_country_iso(scan_dataset("geoip"))
    invalid_geoip_location_code_count = len(id_geoip_location_fail)
    if invalid_geoip_location_code_count is not 0:
        report(id_geoip_location_fail, "test_geoip_location", "test_geoip_location_id_failed_record_ids.json")

    assert invalid_geoip_location_code_count == 0


def test_geoip_continent_name():
    id_geoip_continent_name_fail = check_country_iso(scan_dataset("geoip"))
    invalid_geoip_continent_name_count = len(id_geoip_continent_name_fail)
    if invalid_geoip_continent_name_count is not 0:
        report(id_geoip_continent_name_fail, "test_geoip_continent_name", "test_geoip_continent_name_id_failed_record_ids.json")

    assert invalid_geoip_continent_name_count == 0

