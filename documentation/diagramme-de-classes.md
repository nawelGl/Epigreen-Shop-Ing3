%% a copier-coller dans mermaid.ive

classDiagram
    %% --- REDIS (Panier) ---
    namespace ms_redis_basket {
        class ShoppingCart {
            +String key_id
            +List~CartItem~ items
            +Integer total_items
            +Timestamp expires_at
        }
        class CartItem {
            +Integer product_id
            +String size_label
            +Integer quantity
            +Decimal cached_price_at_add
            +String product_name
        }
    }

    %% --- MICROSERVICE: MEMBERSHIP ---
    namespace ms_membership {
        class Customer {
            +Integer id
            +String email
            +String password_hash
            +String first_name
            +String last_name
        }
        class SavedAddress {
            +Integer id
            +Integer customer_id
            +String street
            +String city
            +String zip_code
            +String country
        }
    }

    %% --- MICROSERVICE: PRODUCT ---
    namespace ms_product {
        %% J'ai remonté Warehouse pour qu'il ne gêne plus le trait
        class Warehouse {
            +Integer id
            +String name
            +String city_location
            +Boolean is_active
        }

        class ProductStock {
            +Integer id
            +Integer product_id
            +String size_label
            +Integer quantity
            +Integer warehouse_id_ref
        }

        class CatalogProduct {
            +Integer id
            +String reference_sku
            +String name
            +Decimal current_price
            +String eco_material_type
            +Float weight_kg
        }
    }

    %% --- MICROSERVICE: ORDER ---
    namespace ms_order {
        class Order {
            +UUID id
            +Integer customer_id
            +Decimal total_price
            +JSONB shipping_address_snapshot
        }
        
        class OrderItem {
            +UUID id
            +UUID order_id
            +Integer product_origin_id
            +String product_name_snapshot
            +Decimal unit_price_snapshot
            +Integer quantity
        }

        class Delivery {
            +UUID id
            +UUID order_id
            +String tracking_number
            +Enum mode
            +Enum status
            +UUID relay_point_id
        }

        class RelayPoint {
            +UUID id
            +String name
            +String address_full
            +Boolean is_active
        }
    }

    %% --- MICROSERVICE: CARBON ---
    namespace ms_carbon {
        class CarbonAudit {
            +UUID id
            +UUID order_id_ref
            +Float co2_emission_calculated
        }
    }

    %% RELATIONS INTERNES
    ShoppingCart *-- CartItem : contains
    Customer "1" *-- "0..*" SavedAddress : owns
    
    %% Relations Product
    ProductStock "0..*" -- "1" Warehouse : stored in
    CatalogProduct "1" *-- "0..*" ProductStock : variants

    Order "1" *-- "1..*" OrderItem : contains
    Order "1" -- "0..1" Delivery : triggers
    Delivery "0..*" -- "0..1" RelayPoint : shipped to
    
    %% LIENS LOGIQUES
    Order ..> Customer : "Link by ID"
    
    OrderItem ..> CatalogProduct : "Copy from (Snapshot)"
    
    CarbonAudit ..> Order : "Audits"