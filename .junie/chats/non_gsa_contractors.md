Yes, you can determine if a contractor is a GSA contract holder using this data.

The FPDS Atom feed provides details about both the immediate contract action and any parent contract vehicle it belongs to. In the database, this information is stored primarily in the `fpds_contract_actions` table and its relationships to the `fpds_agencies` table.

Here is how you can identify GSA contract holders:

### 1. Check the Referenced IDV Agency (`referenced_idv_agency_id`)
When a federal agency places a task order or delivery order against a GSA Schedule or GSA GWAC, the parent contract is known as an Indefinite Delivery Vehicle (IDV).
- If the `referenced_idv_agency_id` matches a GSA agency code (which all start with **`47`**, such as **`4732`** for the Federal Acquisition Service or **`4700`** for General Services Administration), it means the contractor holds a GSA contract vehicle.

### 2. Check the Awarding Agency (`agency_id`)
If the contractor has base contract records (where the record itself is the IDV or a definitive contract) and the `agency_id` resolves to a GSA agency code (starting with **`47`**), they hold a direct GSA contract.

### 3. Look at the Referenced IDV PIID (`referenced_idv_piid`)
The `referenced_idv_piid` column contains the actual contract number of the parent vehicle. GSA Schedule and GWAC contract numbers often follow recognizable patterns (e.g., starting with `GS-`, `47Q`, `47P`). If a contractor's records frequently reference these PIIDs, they are a GSA contract holder.

### 4. IDC / Contract Type Fields
The system also parses fields like `type_of_idc` (Type of Indefinite Delivery Contract) and `multiple_or_single_award_idc`. These fields help indicate if the parent vehicle is a Federal Supply Schedule (FSS), Government-Wide Acquisition Contract (GWAC), or a Multiple Award Contract (MAC) typically managed by GSA.