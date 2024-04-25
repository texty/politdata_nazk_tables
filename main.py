# ! pip install openpyxl xlsxwriter requests pandas tqdm

import os
from datetime import datetime
from main_functions import download_party_info, download_all_reports, party_name_cleaner
from table_functions import table_0_1, table_0_2, table_1, table_2_1, table_2_2, table_3_1, table_3_2, table_3_3, table_3_4, table_3_5, table_4, table_5, table_6, table_7, table_8, table_9_1, table_9_2, table_9_3, table_9_5, table_10, files_where_to_look_for_local_parties


# !!! set to True if you want to update all data from the very beginning
full_update = False


## Download party info
party_list, party_region_list = download_party_info()


## Download all reports
r_df = download_all_reports(full_update)
r_df = party_name_cleaner(r_df, party_variable = 'party_main_name')



## Tables
### Таблиця 0.1: Перевірка на наявність дублів
table_0_1(r_df, full_update)

### Таблиця 1: загальна інформація про партію/осередок
table_1(r_df, full_update)

### Таблиця 0.2: Інформація про те, за який період кожна партія подала звіти
table_0_2()

### Таблиця 2: інформація про осередки політичної партії  
table_2_1(r_df, full_update)

### Таблиця 3: інформація про інші підприємства, установи та організації створені партією політичної партії
table_2_2(r_df, full_update)

### Таблиця 4: зведена інформація про нерухоме майно політичної партії (propertyObjects)
table_3_1(r_df, full_update)

### Таблиця 5: зведена інформація про рухоме майно політичної партії (propertyMovables)
table_3_2(r_df, full_update)

### Таблиця 6: зведена інформація про автотранспорт політичної партії (propertyTransport)
table_3_3(r_df, full_update, party_list, party_region_list)

### Таблиця 7: зведена інформація про цінні папери політичної партії (propertyPapers)
table_3_4(r_df, full_update)

### Таблиця 8: зведена інформація про інше майно та нематеріальні активи політичної партії (propertyNoMoney)
table_3_5(r_df, full_update)

### Таблиця 9: зведена інформація про банківські рахунки політичної партії (propertyMoney)
table_4(r_df, full_update)

### Таблиця 10: зведена інформація про приватні грошові внески на рахунки політичної партії (contributionConMoney)  
table_5(r_df, full_update, party_list, party_region_list)

### Таблиця 11: зведена інформація про негрошові внески на користь політичної партії (contributionOtherCon)
table_6(r_df, full_update, party_list, party_region_list)

### Таблиця 12: зведена інформація про кошти державного фінансування виплачені політичним партіям (contributionCosts)
table_7(r_df, full_update)

### Таблиця 13: зведена інформація про інші грошові надходження на рахунки політичних партій (contributionOtherCosts)
table_8(r_df, full_update)

### Таблиця 14: зведена інформація про витрати державного фінансування політичних партій (paymentGov)
table_9_1(r_df, full_update, party_list, party_region_list)

### Таблиця 15: зведена інформація про витрати з рахунків з приватним фінансуванням (paymentOther)
table_9_2(r_df, full_update, party_list, party_region_list)

### Таблиця 16: зведена інформація про отримання помилкових надходжень (paymentCostsPaymentReceive)
table_9_3(r_df, full_update)

### НЕ ІСНУЄ Таблиця 17: інформація про повернення внесків та помилкових надходжень (paymentCostsPaymentReturn)

### Таблиця 18: інформація про отримання помилкових інших (негрошових) внесків (paymentOtherCostsPaymentReceive)
table_9_5(r_df, full_update)

### НЕ ІСНУЄ Таблиця 19: інформація про отримання помилкових інших (негрошових) внесків (paymentOtherCostsPaymentReturn)

### Таблиця 20: інформація про фінансові зобов'язання (obligate)
table_10(r_df, full_update)


### Перевірити в яких файлах є які партії
files_where_to_look_for_local_parties()



# Додати в readme дату і час останнього оновлення
now = datetime.now().strftime("%Y-%m-%d %H:%M")

with open('README.md', 'r') as file:
    data = file.readlines()
    # replace line wich starts with '**Останнє оновленн' for f"**Останнє оновлення: {now}**\n"
    for i in range(len(data)):
        if data[i].startswith('**Останнє оновленн'):
            data[i] = f"**Останнє оновлення: {now}**\n"
            break
with open('README.md', 'w') as file:
    file.writelines(data)

# запушити на гітхаб
os.system('git add . ; git commit -m "data update"; git push origin main')