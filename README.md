# alfa-subway-client

строится альфа по спроектированому subway сущности Клиент (Client)

источники 
---
1) client 3НФ (3 нормальная форма)
    
    схема данных:

        таблица personal_information, поля:
            1) id -- идентификатор человека
            2) name -- имя человека
            3) birthday -- день рождения человека
            4) age -- возраст человека
        
        таблица phone_numbers, поля:
            1) id -- идентификатор человека
            2) phone - номер телефона человека

        таблица documents, поля:
            1) id -- идентификатор человека
            2) inn -- инн человека
            3) pasport -- паспорт человека

        таблица human_params, поля:
            1) id -- идентификатор человека
            2) weight -- вес человека 
            3) height -- рост человека


2) client star (звезда)
    
    схема данных:
    
        таблица client, поля:
            1) id -- идентификатор человека
            2) name -- имя человека
            3) birthday -- день рождения человека
            4) age -- возраст человека
            5) phone - номер телефона человека
            6) inn -- инн человека
            7) pasport -- паспорт человека
            8) weight -- вес человека 
            9) height -- рост человека
