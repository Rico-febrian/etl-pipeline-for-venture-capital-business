## Transformation Rules

| Column           | Transformation Logic                                                                 |
|------------------|-------------------------------------------------------------------------------------|
| **entity_type**  | `"company"` if object_id starts with "c:"<br>`"fund"` if starts with "f:"<br>`"unknown"` otherwise |
| **address1_clean** | 1. Trim leading/trailing spaces<br>2. Convert to lowercase<br>3. Set to `"unknown"` if NULL/empty |
| **address2_clean** | 1. Trim leading/trailing spaces<br>2. Convert to lowercase<br>3. Set to `"unknown"` if NULL/empty |
| **full_address** | 1. If both addresses empty → `"unknown"`<br>2. If address1 empty → use address2<br>3. If address2 empty → use address1<br>4. Else combine with ", "<br>5. Set to `"unknown"` if combined result is empty |
| **region**       | 1. Trim spaces<br>2. Convert to lowercase<br>3. Set to `"unknown"` if NULL/empty |
| **city**         | 1. Trim spaces<br>2. Convert to lowercase<br>3. Set to `"unknown"` if NULL/empty |
| **country_code** | 1. Trim spaces<br>2. Convert to UPPERCASE<br>3. Set to `"XXX"` if NULL/empty |
| **nk_company_id** | Direct mapping from `object_id` |

## Key Functions
- `clean_address()`: Handles address cleaning logic
- `is_empty()`: Checks for NULL/empty values
- `transform_company()`: Main transformation function
