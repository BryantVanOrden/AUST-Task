
import tkinter as tk
from tkinter import ttk, messagebox
import ldap
import sv_ttk

# --- CONFIGURATION ---
LDAP_CONFIG = {
    "HOST": "ldap://localhost",
    "PORT": 389,
    "ADMIN_DN": "cn=admin,dc=example,dc=com",
    "ADMIN_PW": "admin_password",
    "BASE_DN": "dc=example,dc=com"
}

# --- ROLE DEFINITIONS ---
ROLES = {
    "Student": {"ou": "Students", "group_cn": "students", "gid": 503},
    "Teacher": {"ou": "Teachers", "group_cn": "teachers", "gid": 502},
    "Manager": {"ou": "Managers", "group_cn": "managers", "gid": 501}
}

class LdapUserApp(tk.Tk):
    """The main application window for managing LDAP users."""

    def __init__(self):
        super().__init__()

        # --- Apply the theme ---
        sv_ttk.set_theme("dark")

        self.title("AUST Moodle User Maker")
        self.geometry("500x580") # Increased height for header
        self.resizable(False, False)

        # --- Styling for Header ---
        self._setup_styles()

        # --- Live Update Variables ---
        self.first_name_var = tk.StringVar()
        self.last_name_var = tk.StringVar()
        self.uid_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.telephone_var = tk.StringVar()
        self.address_var = tk.StringVar()
        self.uid_number_var = tk.StringVar(value="5000")
        self.role_var = tk.StringVar(value=list(ROLES.keys())[0])

        self.first_name_var.trace_add("write", self._update_and_format_names)
        self.last_name_var.trace_add("write", self._update_and_format_names)

        self.create_widgets()
        
        # --- Center the window on launch ---
        self._center_window()

    def _setup_styles(self):
        """Configures custom styles for widgets."""
        style = ttk.Style()
        style.configure("Title.TLabel", font=("Segoe UI", 24, "bold"), foreground="orange")
        style.configure("Subtitle.TLabel", font=("Segoe UI", 12), foreground="orange")

    def _center_window(self):
        """Centers the main window on the user's screen."""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')

    def _update_and_format_names(self, *args):
        """Auto-formats names and generates the UID in real-time."""
        first = self.first_name_var.get().capitalize()
        last = self.last_name_var.get().capitalize()

        if self.focus_get() and isinstance(self.focus_get(), ttk.Entry):
            entry_widget = self.focus_get()
            cursor_pos = entry_widget.index(tk.INSERT)
            if entry_widget == self.first_name_entry:
                self.first_name_var.set(first)
                entry_widget.icursor(cursor_pos)
            elif entry_widget == self.last_name_entry:
                self.last_name_var.set(last)
                entry_widget.icursor(cursor_pos)

        if first and last:
            uid = (first[0] + last).lower().replace(" ", "")
            self.uid_var.set(uid)
        else:
            self.uid_var.set("")

    def create_widgets(self):
        """Create and lay out all the UI elements."""
        main_frame = ttk.Frame(self, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        # --- Header ---
        ttk.Label(main_frame, text="AUST", style="Title.TLabel").grid(row=0, column=0, pady=(0, 0))
        ttk.Label(main_frame, text="Moodle User Maker", style="Subtitle.TLabel").grid(row=1, column=0, pady=(0, 20))

        # --- Personal Info Section ---
        personal_frame = ttk.LabelFrame(main_frame, text="Personal Information", padding="10")
        personal_frame.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        personal_frame.columnconfigure(1, weight=1)

        ttk.Label(personal_frame, text="First Name:").grid(row=0, column=0, sticky="w", pady=5)
        self.first_name_entry = ttk.Entry(personal_frame, textvariable=self.first_name_var)
        self.first_name_entry.grid(row=0, column=1, sticky="ew")

        ttk.Label(personal_frame, text="Last Name:").grid(row=1, column=0, sticky="w", pady=5)
        self.last_name_entry = ttk.Entry(personal_frame, textvariable=self.last_name_var)
        self.last_name_entry.grid(row=1, column=1, sticky="ew")

        ttk.Label(personal_frame, text="Telephone:").grid(row=2, column=0, sticky="w", pady=5)
        self.telephone_entry = ttk.Entry(personal_frame, textvariable=self.telephone_var)
        self.telephone_entry.grid(row=2, column=1, sticky="ew")

        ttk.Label(personal_frame, text="Address:").grid(row=3, column=0, sticky="w", pady=5)
        self.address_entry = ttk.Entry(personal_frame, textvariable=self.address_var)
        self.address_entry.grid(row=3, column=1, sticky="ew")

        # --- Account Details Section ---
        account_frame = ttk.LabelFrame(main_frame, text="Account Details", padding="10")
        account_frame.grid(row=3, column=0, sticky="ew")
        account_frame.columnconfigure(1, weight=1)

        ttk.Label(account_frame, text="User ID (uid):").grid(row=0, column=0, sticky="w", pady=5)
        self.uid_entry = ttk.Entry(account_frame, textvariable=self.uid_var, state="readonly")
        self.uid_entry.grid(row=0, column=1, sticky="ew")

        ttk.Label(account_frame, text="Password:").grid(row=1, column=0, sticky="w", pady=5)
        self.password_entry = ttk.Entry(account_frame, textvariable=self.password_var, show="*")
        self.password_entry.grid(row=1, column=1, sticky="ew")

        ttk.Label(account_frame, text="UID Number:").grid(row=2, column=0, sticky="w", pady=5)
        self.uid_number_entry = ttk.Entry(account_frame, textvariable=self.uid_number_var)
        self.uid_number_entry.grid(row=2, column=1, sticky="ew")

        ttk.Label(account_frame, text="Role:").grid(row=3, column=0, sticky="w", pady=5)
        self.role_combobox = ttk.Combobox(account_frame, textvariable=self.role_var, values=list(ROLES.keys()), state="readonly")
        self.role_combobox.grid(row=3, column=1, sticky="ew")

        # --- Action Buttons ---
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=4, column=0, pady=(20, 10))
        ttk.Button(button_frame, text="Add User", command=self.add_user, style="Accent.TButton").pack(side="left", padx=5)
        ttk.Button(button_frame, text="Clear Form", command=self.clear_form).pack(side="left", padx=5)
        
        # --- Status Bar ---
        status_frame = ttk.Frame(main_frame, padding=(5, 2))
        status_frame.grid(row=5, column=0, sticky="ew", pady=(10, 0))
        self.status_label = ttk.Label(status_frame, text="Status: Ready")
        self.status_label.pack(side="left")

    def add_user(self):
        # ... (rest of the add_user method is unchanged)
        first = self.first_name_var.get().strip()
        last = self.last_name_var.get().strip()
        pw = self.password_var.get().strip()
        phone = self.telephone_var.get().strip()
        addr = self.address_var.get().strip()
        uid_num_str = self.uid_number_var.get().strip()
        role = self.role_var.get()
        uid = self.uid_var.get().strip()
        full_name = f"{first} {last}"

        if not all([first, last, pw, uid, uid_num_str]):
            messagebox.showerror("Error", "First Name, Last Name, Password, and UID Number are required.")
            return

        try:
            uid_num = int(uid_num_str)
        except ValueError:
            messagebox.showerror("Error", "UID Number must be an integer.")
            return

        self.status_label.config(text="Status: Connecting...")
        self.update_idletasks()

        role_info = ROLES[role]
        user_dn = f"uid={uid},ou={role_info['ou']},ou=Users,{LDAP_CONFIG['BASE_DN']}"
        group_dn = f"cn={role_info['group_cn']},ou=Groups,{LDAP_CONFIG['BASE_DN']}"

        ldif = [
            ('objectClass', [b'inetOrgPerson', b'posixAccount', b'top']),
            ('cn', [full_name.encode()]), ('sn', [last.encode()]),
            ('givenName', [first.encode()]), ('uid', [uid.encode()]),
            ('uidNumber', [str(uid_num).encode()]), ('gidNumber', [str(role_info['gid']).encode()]),
            ('homeDirectory', [f'/home/users/{uid}'.encode()]), ('title', [role.encode()]),
            ('mail', [f'{uid}@exsample.com'.encode()]), ('userPassword', [pw.encode()]),
            ('telephoneNumber', [phone.encode()]), ('postalAddress', [addr.encode()])
        ]

        try:
            conn = ldap.initialize(LDAP_CONFIG['HOST'])
            conn.simple_bind_s(LDAP_CONFIG['ADMIN_DN'], LDAP_CONFIG['ADMIN_PW'])
            conn.add_s(user_dn, ldif)
            mod_attrs = [(ldap.MOD_ADD, 'memberUid', [uid.encode()])]
            conn.modify_s(group_dn, mod_attrs)
            
            self.status_label.config(text=f"Status: Success!")
            messagebox.showinfo("Success", f"User '{full_name}' was created and added to the '{role_info['group_cn']}' group.")
            self.clear_form()
            self.uid_number_var.set(str(uid_num + 1))

        except ldap.LDAPError as e:
            self.status_label.config(text="Status: Error!")
            messagebox.showerror("LDAP Error", f"An error occurred:\n{e}")
        finally:
            if 'conn' in locals():
                conn.unbind_s()


    def clear_form(self):
        """Clears all entry fields by resetting the StringVars."""
        self.first_name_var.set("")
        self.last_name_var.set("")
        self.password_var.set("")
        self.telephone_var.set("")
        self.address_var.set("")
        self.first_name_entry.focus()


if __name__ == "__main__":
    app = LdapUserApp()
    app.mainloop()