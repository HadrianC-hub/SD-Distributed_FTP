import streamlit as st
import os
import sys
import io
import time
import re
import time
import client as client

st.set_page_config(page_title="Cliente FTP", page_icon="📂", layout="wide")


# -----------------------------------------------------------------------------------------------------
# Estado inicial

# -----------------------------------------------------------------------------------------------------
if "console" not in st.session_state:
    st.session_state.console = []
if "show_console" not in st.session_state:
    st.session_state.show_console = False
if "ftp_client" not in st.session_state:
    st.session_state.ftp_client = None
if "current_dir" not in st.session_state:
    st.session_state.current_dir = "/"
if "navigation_lock" not in st.session_state:
    st.session_state.navigation_lock = False
if "connection_id" not in st.session_state:
    st.session_state.connection_id = None
if "delete_candidate" not in st.session_state:
    st.session_state.delete_candidate = None  # (name, type)
if "creating_folder" not in st.session_state:
    st.session_state.creating_folder = False
if "new_folder_name" not in st.session_state:
    st.session_state.new_folder_name = ""
if "download_candidate" not in st.session_state:
    st.session_state.download_candidate = None  # (name, type)
if "download_path" not in st.session_state:
    st.session_state.download_path = "downloads"
if "transfer_mode" not in st.session_state:
    st.session_state.transfer_mode = 'S'  # Por defecto Stream
if "transfer_type" not in st.session_state:
    st.session_state.transfer_type = 'A'  # Por defecto ASCII
if "renaming_candidate" not in st.session_state:
    st.session_state.renaming_candidate = None  # (name, type)
if "new_name" not in st.session_state:
    st.session_state.new_name = ""
if "download_port_candidate" not in st.session_state:
    st.session_state.download_port_candidate = None  # (name, type)
if "using_port_mode" not in st.session_state:
    st.session_state.using_port_mode = False
if "upload_candidate" not in st.session_state:
    st.session_state.upload_candidate = None
if "upload_path" not in st.session_state:
    st.session_state.upload_path = ""
if "uploading" not in st.session_state:
    st.session_state.uploading = False
if "stou_upload_candidate" not in st.session_state:
    st.session_state.stou_upload_candidate = None
if "stou_upload_path" not in st.session_state:
    st.session_state.stou_upload_path = ""
if "stou_uploading" not in st.session_state:
    st.session_state.stou_uploading = False
if "stou_multiple_files" not in st.session_state:
    st.session_state.stou_multiple_files = []
if "append_candidate" not in st.session_state:
    st.session_state.append_candidate = None  # (name, type)
if "append_local_path" not in st.session_state:
    st.session_state.append_local_path = ""
if "appending" not in st.session_state:
    st.session_state.appending = False
if "_need_rerun" not in st.session_state:
    st.session_state["_need_rerun"] = False
if "_last_rerun_time" not in st.session_state:
    st.session_state["_last_rerun_time"] = 0.0
