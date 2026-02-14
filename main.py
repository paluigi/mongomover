import time
from pymongo import MongoClient
import flet as ft


def main(page: ft.Page) -> None:
    VNUM = "2026.01"
    def mongomove(
        e
        # origin_connection: str,
        # origin_db: str,
        # origin_coll: str,
        # destination_connection: str,
        # destination_db: str,
        # destination_coll: str,
        # batch_size: int = 100,
        # delete: bool = False
    )    -> int:  
        """
        Transfer documents from one MongoDB collection to another.

        Args:
            origin_connection (str): Connection string for the source MongoDB instance.
            origin_db (str): Name of the database containing the source collection.
            origin_coll (str): Name of the source collection.
            destination_connection (str): Connection string for the destination MongoDB instance.
            destination_db (str): Name of the database containing the destination collection.
            destination_coll (str): Name of the destination collection.
            batch_size (int, optional): Number of documents to transfer in each batch. Defaults to 100.
            delete (bool, optional): Whether to delete documents from the source collection after transfer. Defaults to False.

        Returns:
            int: An integer representing the total number of records transferred.

        Notes:
            This function connects to both MongoDB instances, transfers documents in batches,
            and closes the connections when finished.
        """
        if not origin_conn.value:
            origin_conn.error_text = "This field is required"
            page.update()
        if not origin_db.value:
            origin_db.error_text = "This field is required"
            page.update()
        if not origin_coll.value:
            origin_coll.error_text = "This field is required"
            page.update()
        if not dest_conn.value:
            dest_conn.error_text = "This field is required"
            page.update()
        if not dest_db.value:
            dest_db.error_text = "This field is required"
            page.update()
        if not dest_coll.value:
            dest_coll.error_text = "This field is required"
            page.update()
        if not batch_size.value:
            batch_size.error_text = "This field is required"
            page.update()
        # Disable the transfer button
        trigger.disabled = True
        page.update()
        # Connect to the source MongoDB
        try:
            source_client = MongoClient(origin_conn.value)
            source_db = source_client[origin_db.value]
            source_col = source_db[origin_coll.value]
            total_count = source_col.count_documents({})
        except Exception:
            trigger.disabled = False
            page.update()
            source_error = ft.AlertDialog(
                title=ft.Text("Source DB Connection Error"),
            )
            page.show_dialog(source_error)
            return 1
        # Get the total count of documents to be moved
        details.controls = []
        
        if total_count == 0:
            trigger.disabled = False
            details.controls.append(ft.Text("No documents to be moved!"))
            source_client.close()
            page.update()
            return 1
        details.controls.append(
            ft.Text(f"Total number of documents to be moved: {total_count}"))
        pb = ft.ProgressBar(width=400, value = 0)
        details.controls.append(pb)
        page.update()
        n_runs = (total_count // batch_size.value) + 1
        run_pct = round(1/n_runs, 4)
        
        # Connect to the destination MongoDB
        try:
            destination_client = MongoClient(dest_conn.value)
            destination_db = destination_client[dest_db.value]
            destination_col = destination_db[dest_coll.value]
            # Operation needed to check the connection
            _ = destination_col.count_documents({})
        except Exception:
            source_client.close()
            trigger.disabled = False
            details.controls = []
            page.update()
            destination_error = ft.AlertDialog(
                title=ft.Text("Destination DB Connection Error"),
            )
            page.show_dialog(destination_error)
            return 1

        # Counter for the number of documents transferred
        counter = 0
        # Loop to operate N documents at a time
        while True:
            # Wat one sec to relief the server
            time.sleep(1)
            # Retrieve IDs of documents to be copied
            if delete.value:
                cursor = source_col.find({}, limit=batch_size.value)
            else:
                cursor = source_col.find({}, limit=batch_size.value, skip=counter)
            # Move date to list
            records = [doc for doc in cursor]
            # Break if no more records
            if len(records) == 0:
                break
            # separate ids and data
            copied_ids = [doc.get("_id") for doc in records]
            batch = []
            for doc in records:
                doc.pop("_id", None)
                batch.append(doc)
            # Insert documents in target collection
            destination_col.insert_many(batch)
            # Delete documents from source collection if flagged
            if delete.value:
                source_col.delete_many({'_id': {'$in': copied_ids}})
            # Increment the counter
            counter += len(copied_ids)
            # Update the progress bar
            pb.value += run_pct
            page.update()
        # Close connections
        source_client.close()
        destination_client.close()
        # Enable the transfer button
        trigger.disabled = False
        details.controls.append(ft.Text("Transfer complete!"))
        page.update()
        # return number of records transferred
        return counter

    origin_conn = ft.TextField(label="Connection string for the source MongoDB instance", expand=True)
    origin_db = ft.TextField(label="Name of the database containing the source collection", expand=True)
    origin_coll = ft.TextField(label="Name of the source collection", expand=True)
    dest_conn = ft.TextField(label="Connection string for the destination MongoDB instance", expand=True)
    dest_db = ft.TextField(label="Name of the database containing the destination collection", expand=True)
    dest_coll = ft.TextField(label="Name of the destination collection", expand=True)
    batch_size = ft.TextField(
        label="Number of documents to transfer in each batch",
        input_filter=ft.InputFilter(allow=True, regex_string=r"^[0-9]*$", replacement_string=""),
        value=100,
        expand=True
    )
    delete = ft.Checkbox(label="Delete documents from the source collection after transfer?")
    trigger = ft.Button("Transfer", on_click=mongomove)
    details = ft.Column([])
    disclaimer = ft.Text("This tool is provided AS-IS, without warranty of any kind. Use at your own risk.", size=12)
    credits = ft.Row([
        ft.Text("Source code:", size=12),
        ft.Button(
            content="GitHub",
            url="https://github.com/paluigi/mongomover")
    ])
    version = ft.Text(f"Version: {VNUM}", size=12)

    page.add(
        ft.SafeArea(
            ft.Column(
                [
                ft.Text("Mongomover", size=50, weight=ft.FontWeight.BOLD),
                ft.Text("Transfer records between MongoDB instances"),
                ft.Row([
                    ft.Column(
                        width=220,
                        expand=True,
                        controls=[
                        ft.Text("Source collection", size=25, weight=ft.FontWeight.BOLD),
                        origin_conn,
                        origin_db,
                        origin_coll,
                    ]),
                    ft.VerticalDivider(width=50),
                    ft.Column(
                        width=220,
                        expand=True,
                        controls=[
                        ft.Text(value="Destination collection", size=25, weight=ft.FontWeight.BOLD),
                        dest_conn,
                        dest_db,
                        dest_coll,
                    ])

                ]),
                batch_size,
                delete, 
                trigger,
                details,
                ft.Divider(height=20),
                disclaimer,
                credits,
                version
            ])
        )
    )
    
    # Make the page scrollable
    page.scroll = ft.ScrollMode.ADAPTIVE
    page.update()

ft.run(main)
