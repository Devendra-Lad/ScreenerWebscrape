from external.nse import NSE
import sqlite3

nse = NSE()

connect = sqlite3.connect("data/database.db")
cursor = connect.cursor()

try:
    date, data = nse.fetch_fno_oi_data_csv(0)

    client = data[2]
    dii = data[3]
    fii = data[4]
    pro = data[5]
    total = data[6]

    # FII Data
    fii_fi_long = float(fii[1])
    fii_fi_short = float(fii[2])

    fii_fi_r = fii_fi_long / (fii_fi_long + fii_fi_short) * 100

    fii_fs_long = float(fii[3])
    fii_fs_short = float(fii[4])

    fii_io_call_long = float(fii[5])
    fii_io_put_long = float(fii[6])

    fii_io_call_short = fii[7]
    fii_io_put_short = fii[8]

    fii_so_call_long = fii[9]
    fii_so_put_long = fii[10]

    fii_so_call_short = fii[11]
    fii_so_put_short = fii[12]

    # DII Data
    dii_fi_long = dii[1]
    dii_fi_short = dii[2]

    dii_fs_long = dii[3]
    dii_fs_short = dii[4]

    dii_io_call_long = dii[5]
    dii_io_put_long = dii[6]

    dii_io_call_short = dii[7]
    dii_io_put_short = dii[8]

    dii_so_call_long = dii[9]
    dii_so_put_long = dii[10]

    dii_so_call_short = dii[11]
    dii_so_put_short = dii[12]

    # Client Data
    client_fi_long = client[1]
    client_fi_short = client[2]

    client_fs_long = client[3]
    client_fs_short = client[4]

    client_io_call_long = client[5]
    client_io_put_long = client[6]

    client_io_call_short = client[7]
    client_io_put_short = client[8]

    client_so_call_long = client[9]
    client_so_put_long = client[10]

    client_so_call_short = client[11]
    client_so_put_short = client[12]

    # PRO Data
    pro_fi_long = pro[1]
    pro_fi_short = pro[2]

    pro_fs_long = pro[3]
    pro_fs_short = pro[4]

    pro_io_call_long = pro[5]
    pro_io_put_long = pro[6]

    pro_io_call_short = pro[7]
    pro_io_put_short = pro[8]

    pro_so_call_long = pro[9]
    pro_so_put_long = pro[10]

    pro_so_call_short = pro[11]
    pro_so_put_short = pro[12]

    date_formatted = date.strftime('%Y-%m-%d')
    cursor.execute('''
    INSERT INTO FNO_CONSOLIDATED(
        date,
        fii_fi_long,
        fii_fi_short,
        fii_fi_r,
        fii_fs_long,
        fii_fs_short,
        fii_io_call_long,
        fii_io_put_long,
        fii_io_call_short,
        fii_io_put_short,
        fii_so_call_long,
        fii_so_put_long,
        fii_so_call_short,
        fii_so_put_short,
        dii_fi_long,
        dii_fi_short,
        dii_fs_long,
        dii_fs_short,
        dii_io_call_long,
        dii_io_put_long,
        dii_io_call_short,
        dii_io_put_short,
        dii_so_call_long,
        dii_so_put_long,
        dii_so_call_short,
        dii_so_put_short,
        client_fi_long,
        client_fi_short,
        client_fs_long,
        client_fs_short,
        client_io_call_long,
        client_io_put_long,
        client_io_call_short,
        client_io_put_short,
        client_so_call_long,
        client_so_put_long,
        client_so_call_short,
        client_so_put_short,
        pro_fi_long,
        pro_fi_short,
        pro_fs_long,
        pro_fs_short,
        pro_io_call_long,
        pro_io_put_long,
        pro_io_call_short,
        pro_io_put_short,
        pro_so_call_long,
        pro_so_put_long,
        pro_so_call_short,
        pro_so_put_short)  
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (date_formatted,
                    fii_fi_long,
                    fii_fi_short,
                    fii_fi_r,
                    fii_fs_long,
                    fii_fs_short,
                    fii_io_call_long,
                    fii_io_put_long,
                    fii_io_call_short,
                    fii_io_put_short,
                    fii_so_call_long,
                    fii_so_put_long,
                    fii_so_call_short,
                    fii_so_put_short,
                    dii_fi_long,
                    dii_fi_short,
                    dii_fs_long,
                    dii_fs_short,
                    dii_io_call_long,
                    dii_io_put_long,
                    dii_io_call_short,
                    dii_io_put_short,
                    dii_so_call_long,
                    dii_so_put_long,
                    dii_so_call_short,
                    dii_so_put_short,
                    client_fi_long,
                    client_fi_short,
                    client_fs_long,
                    client_fs_short,
                    client_io_call_long,
                    client_io_put_long,
                    client_io_call_short,
                    client_io_put_short,
                    client_so_call_long,
                    client_so_put_long,
                    client_so_call_short,
                    client_so_put_short,
                    pro_fi_long,
                    pro_fi_short,
                    pro_fs_long,
                    pro_fs_short,
                    pro_io_call_long,
                    pro_io_put_long,
                    pro_io_call_short,
                    pro_io_put_short,
                    pro_so_call_long,
                    pro_so_put_long,
                    pro_so_call_short,
                    pro_so_put_short))

    cursor.execute('''
    INSERT INTO THOUSANDFEET(date, fii_fi_r) 
    values (?, ?) ON CONFLICT(date)
    DO UPDATE SET fii_fi_r=?
    WHERE date=?''', (date_formatted, format(fii_fi_r, '.2f'), format(fii_fi_r, '.2f'), date_formatted))

except Exception as e:
    print(e)

connect.commit()
connect.close()
