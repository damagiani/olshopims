import pymysql
import time
import json
import os

time.sleep(1)
print("===== INTEGRATION ENGINE ONLINE SHOP X BANK =====\n")
while (1):
    try:
        connection_to_bank = 1

        try:
            connOlshop = pymysql.connect(host='103.55.39.44', user='usfmyid_wikan', passwd='wikanwikan', db='usfmyid_olshopnew', port=3306)
            curOlshop = connOlshop.cursor()
        except:
            print("FAILED CONNECT TO ONLINE SHOP")

        try:
            connBank = pymysql.connect(host='remotemysql.com', user='CIVh0nHZ1z', passwd='BC2nK3R6k2', db='CIVh0nHZ1z', port=3306)
            curBank = connBank.cursor()
        except:
            print("FAILED CONNECT TO BANK")
            connection_to_bank = 0

        sql_select = "SELECT * FROM tb_invoice"
        curOlshop.execute(sql_select)
        invoice = curOlshop.fetchall()

        sql_select = "SELECT * FROM tb_integrasi"
        curOlshop.execute(sql_select)
        integrasi = curOlshop.fetchall()

        print("TOTAL ROW ON INVOICE = %d || INTEGRASI = %d" % (len(invoice), len(integrasi)))

        # insert listener
        if (len(invoice) > len(integrasi)):
            print("\n===== INSERT DATA DETECTED =====")
            for data in invoice:
                a = 0
                for dataIntegrasi in integrasi:
                    if (data[0] == dataIntegrasi[0]):
                        a = 1
                if (a == 0):
                    print("----- RUN INSERT FOR ID = %s -----\n" % (data[0]))
                    val = (data[0], data[1], data[2], data[3])
                    insert_integrasi_olshop = "insert into tb_integrasi (id_invoice, tanggal, total_amount, status_transaksi) values(%s,%s,%s,%s)"
                    curOlshop.execute(insert_integrasi_olshop, val)
                    connOlshop.commit()

                    if (connection_to_bank == 1):
                        insert_integrasi_bank = "insert into tb_integrasi (id_invoice, tanggal, total_amount, status_transaksi) values(%s,%s,%s,%s)"
                        curBank.execute(insert_integrasi_bank, val)
                        connBank.commit()

                        insert_invoice_bank = "insert into tb_invoice (id_invoice, tanggal, total_amount, status_transaksi) values(%s,%s,%s,%s)"
                        curBank.execute(insert_invoice_bank, val)
                        connBank.commit()

        # delete listener
        if (len(invoice) < len(integrasi)):
            print("\n===== DELETE DATA DETECTED =====")
            for dataIntegrasi in integrasi:
                a = 0
                for data in invoice:
                    if (dataIntegrasi[0] == data[0]):
                        a = 1
                if (a == 0):
                    print("----- RUN DELETE FOR ID = %s -----\n" % (dataIntegrasi[0]))
                    delete_integrasi_olshop = "delete from tb_integrasi where id_invoice = '%s'" % (dataIntegrasi[0])
                    curOlshop.execute(delete_integrasi_olshop)
                    connOlshop.commit()

                    if (connection_to_bank == 1):
                        delete_integrasi_bank = "delete from tb_integrasi where id_invoice = %s" % (dataIntegrasi[0])
                        curBank.execute(delete_integrasi_bank)
                        connBank.commit()

                        delete_invoice_bank = "delete from tb_invoice where id_invoice = %s" % (dataIntegrasi[0])
                        curBank.execute(delete_invoice_bank)
                        connBank.commit()

        # update listener
        if (invoice != integrasi):
            print("\n===== UPDATE DATA DETECTED =====")
            for data in invoice:
                for dataIntegrasi in integrasi:
                    if (data[0] == dataIntegrasi[0]):
                        if (data != dataIntegrasi):
                            val = (data[1], data[2], data[3], data[0])
                            print("----- RUN UPDATE FOR ID = %s -----\n" % (data[0]))
                            update_integrasi_olshop = "update tb_integrasi set tanggal = %s, total_amount = %s, status_transaksi = %s where id_invoice = %s"
                            curOlshop.execute(update_integrasi_olshop, val)
                            connOlshop.commit()

                            if (connection_to_bank == 1):
                                update_integrasi_bank = "update tb_integrasi set tanggal = %s, total_amount = %s, status_transaksi = %s where id_invoice = %s"
                                curBank.execute(update_integrasi_bank, val)
                                connBank.commit()

                                update_invoice_bank = "update tb_invoice set tanggal = %s, total_amount = %s, status_transaksi = %s where id_invoice = %s"
                                curBank.execute(update_invoice_bank, val)
                                connBank.commit()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    # delay
    time.sleep(30)
