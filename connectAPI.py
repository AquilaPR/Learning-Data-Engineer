import requests
import json
import mysql.connector
from datetime import datetime, timedelta
import pytz
import time

# Konfigurasi database
db_config = {
    'host': 'localhost',  # Ganti dengan host database Anda
    'user': 'root',  # Ganti dengan username database Anda
    'password': '',  # Ganti dengan password database Anda
    'database': 'data_bridge'  # Ganti dengan nama database Anda
}

# Fungsi untuk mengambil token dari API
def ambil_token():
    baseUrl = "https://api.millehub.com/v1/oauth2/token"
    header = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}
    data = {
        'clientId': "65cccb27-7d37-4b10-8bdf-139ff7393cf0",
        'signature': '84641ef4842e09382009fc5395b4233d14a35d3d2399ed0a59bb162815a2998d',
        'grantType': 'client_credentials',
        'scope': 'mille.transaction'
    }
    respon = requests.post(url=baseUrl, headers=header, data=json.dumps(data))
    hasil = json.loads(respon.text)
    return hasil

# Fungsi untuk mengonversi waktu ISO ke UTC
def iso_to_utc(ISO8601stringFormat):
    return datetime.fromisoformat(ISO8601stringFormat).astimezone(pytz.timezone("Asia/Jakarta"))

# Fungsi untuk mengonversi waktu UTC ke ISO
def utc_to_iso(datetimeFormat):
    return datetimeFormat.astimezone(pytz.timezone("UTC")).isoformat().replace("+00:00", "Z")

# Fungsi untuk mengambil waktu terakhir dari database
def ambil_waktu_akhir_dari_db():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Asumsikan Anda menyimpan data waktu transaksi di kolom 'waktu' pada tabel 'coba_items'
    query = "SELECT MAX(waktu) FROM coba_items"
    cursor.execute(query)
    hasil = cursor.fetchone()

    cursor.close()
    conn.close()

    if hasil[0] is not None:
        return hasil[0].astimezone(pytz.timezone("Asia/Jakarta"))
    else:
        # Jika tidak ada data, default ke satu hari sebelumnya
        return datetime.now().astimezone(pytz.timezone("Asia/Jakarta")) - timedelta(days=1)

# Fungsi untuk mengambil data dari API
def ambil_data(token, kunci, awal_waktu, akhir_waktu):
    kunciToken = token['accessToken']
    tipe_token = token['tokenType']
    baseUrl = 'https://api.millehub.com/v1/transaction/query'
    header = {'Content-Type': 'application/json', 'Authorization': tipe_token + ' ' + kunciToken}
    data = {'store': kunci, 'startTimestamp': awal_waktu, 'endTimestamp': akhir_waktu}
    respon = requests.post(url=baseUrl, headers=header, data=json.dumps(data))

    if respon.status_code == 403:
        text_respon = respon.text
        waktu_bisa_request = iso_to_utc(text_respon[:-1])
        print("Error 403, waktu bisa request : " + str(waktu_bisa_request))
        quit()

    elif respon.status_code == 429:
        eksekusi_lagi_dalam = datetime.now() + timedelta(minutes=15) + timedelta(seconds=1)
        print("Error 429, respon : " + str(respon.text))
        print("Eksekusi lagi dalam : " + str(eksekusi_lagi_dalam + timedelta(hours=7)))
        selisih_waktu = eksekusi_lagi_dalam - datetime.now()
        time.sleep(selisih_waktu.total_seconds() + 1)
        return ambil_data(token, kunci, awal_waktu, akhir_waktu)

    hasil = json.loads(respon.text)
    return hasil

# Fungsi untuk menyimpan data ke database
def simpan_ke_db(data_item, data_addon):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Simpan data_item
    item_count = 0
    for item in data_item:
        cursor.execute("""
        INSERT INTO coba_items (outlet, invoice, waktu, status, serving, payment_method, priceBook, jenis, item_name, item_quantity, discount_total, item_sales, item_label, kasir)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, item)
        item_count += 1

    # Simpan data_addon
    addon_count = 0
    for addon in data_addon:
        cursor.execute("""
        INSERT INTO coba_addons (outlet, invoice, waktu, status, kasir, serving, payment_method, priceBook, addon_name, addon_id, quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, addon)
        addon_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    # Output transaksi terakhir yang berhasil disimpan
    if data_item:
        print("\nTransaksi terakhir yang disimpan ke database:")
        last_item = data_item[-1]
        print(f"Outlet: {last_item[0]}, Invoice: {last_item[1]}, Waktu: {last_item[2]}, Item: {last_item[8]}, Quantity: {last_item[9]}, Total Sales: {last_item[11]}")
    else:
        print("Tidak ada transaksi baru yang disimpan.")

    # Output jumlah data yang ditambahkan
    print(f"\nJumlah data baru yang ditambahkan ke database: {item_count} items dan {addon_count} addons.")

# Kunci outlet
kunci_outlet = {
    'Cianjur': '53438d3e4c9a11eea077b4969187a0d0',
    'Antapani': '5bc250dd4c9a11eea077b4969187a0d0',
    'Arcamanik': 'a0e282a0a3b611ee8b64000c29af2617',
    'Badami': 'a0e280b3a3b611ee8b64000c29af2617',
    'Bakmi Yan': '820f0f88085711ef8816000c29af2617',
}

# Fungsi utama
def main():
    token = ambil_token()

    # Ambil waktu terakhir dari database
    waktu_terakhir_db = ambil_waktu_akhir_dari_db()
    waktu_sekarang = utc_to_iso(datetime.now().astimezone(pytz.timezone("Asia/Jakarta")))

    # Cetak waktu untuk verifikasi
    waktu_awal_jakarta = waktu_terakhir_db.astimezone(pytz.timezone("Asia/Jakarta"))
    waktu_akhir_jakarta = datetime.now().astimezone(pytz.timezone("Asia/Jakarta"))
    print(f"Rentang waktu yang digunakan - Awal: {waktu_awal_jakarta}, Akhir: {waktu_akhir_jakarta}")

    # Ambil data dan simpan ke database
    data_item = []
    data_addon = []

    for nama_outlet in kunci_outlet.keys():
        awal_waktu_iso = utc_to_iso(waktu_terakhir_db)  # Gunakan waktu terakhir dari database sebagai awal_waktu
        transaksi_outlet = ambil_data(token, kunci_outlet[nama_outlet], awal_waktu_iso, waktu_sekarang)

        for transaction in transaksi_outlet:
            outlet = nama_outlet
            invoice = transaction["invoice"]
            waktu = str(iso_to_utc(transaction["openTime"][:-1])).replace("+07:00", "")
            status = transaction["status"]
            priceBook = transaction.get("priceBook", "Default")
            serving = transaction["serving"]
            kasir = transaction["openUser"]
            payment_method = transaction.get("payments", [{}])[0].get("method", "")
            diskon = str(transaction["discountTotal"])

            if "items" in transaction:
                for item in transaction["items"]:
                    nama_item = item["name"]
                    quantity = str(int(item["quantity"]))
                    sales = str(int(item["totalPrice"]))
                    label = 'Label default item'  # Ganti jika Anda memiliki label
                    data_item.append([outlet, invoice, waktu, status, serving, payment_method, priceBook, "item", nama_item, quantity, diskon, sales, label, kasir])

                    if "addOns" in item:
                        for addon in item["addOns"]:
                            data_addon.append([outlet, invoice, waktu, status, kasir, serving, payment_method, priceBook, addon["name"], addon["id"], quantity])

            if "bundles" in transaction:
                for bundle in transaction["bundles"]:
                    nama_item = bundle["name"]
                    quantity = str(int(bundle["quantity"]))
                    sales = str(int(bundle["totalPrice"]))
                    label = 'Label default bundle'  # Ganti jika Anda memiliki label
                    data_item.append([outlet, invoice, waktu, status, serving, payment_method, priceBook, "bundle", nama_item, quantity, diskon, sales, label, kasir])

                    for item in bundle.get("items", []):
                        if "addOns" in item:
                            for addon in item["addOns"]:
                                data_addon.append([outlet, invoice, waktu, status, kasir, serving, payment_method, priceBook, addon["name"], addon["id"], quantity])

    simpan_ke_db(data_item, data_addon)
    print("Data berhasil disimpan ke database.")

# Jalankan fungsi utama
if __name__ == "__main__":
    main()
