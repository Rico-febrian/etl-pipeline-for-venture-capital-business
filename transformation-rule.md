# **Source to Target Mapping & Transformation Rules**

# Transformation Rules for dim_company Table

## 📄 Transformation Rules for `dim_company`

| Source Column(s)                  | Target Column     | Transformation Rule                                                                 |
|-----------------------------------|-------------------|-------------------------------------------------------------------------------------|
| *Generated*                       | `sk_company_id`   | Surrogate key, auto generated using BIGSERIAL                                       |
| `company.object_id`               | `nk_company_id`   | Natural key, direct mapping from source and rename                                  |
| `company.object_id`               | `entity_type`     | If starts with `'c:'`, set to `'company'`; if `'f:'`, set to `'fund'`; else `NULL`  |
| `company.address1`, `address2`    | `full_address`    | Clean both and concatenate with comma separator. If both are NULL/empty, set to `NULL`. If one is NULL/empty, use the other only |
| `company.region`                  | `region`          | Convert to lowercase; if NULL/empty after cleaning, set to `NULL`                   |
| `company.city`                    | `city`            | Convert to lowercase; if NULL/empty after cleaning, set to `NULL`                   |
| `company.country_code`            | `country_code`    | Convert to uppercase; if NULL/empty after cleaning, set to `NULL`                   |
| *Generated*                       | `created_at`      | Auto-generated timestamp of row creation                                            |
| *Generated*                       | `updated_at`      | Auto-generated timestamp of row update                                              |

---
---

## 📄 Transformation Rules for `dim_people`

| Source Column(s)                  | Target Column     | Transformation Rule                                                                 |
|-----------------------------------|-------------------|-------------------------------------------------------------------------------------|
| *Generated*                       | `sk_people_id`    | Surrogate key, auto generated using BIGSERIAL                                       |
| `people.object_id`                | `nk_people_id`    | Natural key, direct mapping from source and rename                                  |
| `people.first_name`, `last_name`  | `full_name`       | Clean both and concatenate with space separator. If both are NULL/empty, set to `NULL`. If one is NULL/empty, use the other only | 
| `people.affiliation_name`         | `affiliation_name`| Convert to lowercase; if NULL/empty after cleaning, set to `NULL`                   |
| *Generated*                       | `created_at`      | Auto-generated timestamp of row creation                                            |
| *Generated*                       | `updated_at`      | Auto-generated timestamp of row update                                              |

---
---

## 📄 Transformation Rules for `dim_funds`

| Source Column(s)                  | Target Column     | Transformation Rule                                                                 |
|-----------------------------------|-------------------|-------------------------------------------------------------------------------------|
| *Generated*                       | `sk_fund_id`      | Surrogate key, auto generated using BIGSERIAL                                       |
| `fund.object_id`                  | `nk_fund_id`      | Natural key, direct mapping from source and rename                                  |
| `fund.name`                       | `fund_name`       | Convert to lowercase and rename                                                     |
| `fund.raised_amount`, `company.raised_currency_code`  | `raised_amount_usd`| Convert all currency amount into USD using exchange rate logic |
| `fund.funded_at`                  | `funded_at`       | Convert to `yyyyMMdd` format and cast to integer. Lookup to `dim_date.date_id` as Foreign Key |
| `fund.source_description`         | `fund_description`| Convert to lowercase; if NULL/empty after cleaning, set to `NULL`                   |
| *Generated*                       | `created_at`      | Auto-generated timestamp of row creation                                            |
| *Generated*                       | `updated_at`      | Auto-generated timestamp of row update                                              |

---
---

## 📄 Transformation Rules for `bridge_company_people`

| Source Column(s)                  | Target Column     | Transformation Rule                                                                 |
|-----------------------------------|-------------------|-------------------------------------------------------------------------------------|
| *Generated*                       | `sk_company_people_id` | Surrogate key, auto generated using BIGSERIAL                                  |   
| `relationships.person_object_id`  | `sk_company_id`   | Lookup join with `dim_company` on `nk_company_id = relationship_object_id` to get surrogate key |        | `relationships.relationship_object_id`| `sk_people_id`| Lookup join with `dim_people` on `nk_people_id = person_object_id` to get surrogate key |
| `relationships.title`             | `title`           | Convert to lowercas. If value is `'.'`, set to NULL                                 |
| `relationships.is_past`           | `is_past`         | Direct mapping                                                                      |
| `relationships.start_at`          | `relationship_start_at`| Rename, convert to `yyyyMMdd` formatm cast to integer. Lookup to `dim_date.date_id` as Foreign Key |
| `relationships.end_at`            | `relationship_end_at`  | Rename, convert to `yyyyMMdd` format, cast to integer. Lookup to `dim_date.date_id` as Foreign Key |
| *Generated*                       | `created_at`      | Auto-generated timestamp of row creation                                            |
| *Generated*                       | `updated_at`      | Auto-generated timestamp of row update                                              |
