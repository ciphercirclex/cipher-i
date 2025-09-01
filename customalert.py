import tkinter as tk

class CustomAlert:
    def __init__(self, title="Cipher", message="An unexpected error occurred."):
        """
        Initialize a compact custom alert window with a centered 'Cipher' header.
        Minimize, maximize, and close buttons are completely removed.
        
        Args:
            title (str): Title of the alert window (default: 'Cipher').
            message (str): Message to display, provided by the caller.
        """
        self.root = tk.Tk()
        self.root.title(title)  # Title is set but won't be visible due to overrideredirect
        self.root.resizable(False, False)
        self.root.configure(bg="#FFFFFF")  # White background

        # Make the window stay on top of all other windows
        self.root.attributes('-topmost', True)

        # Remove all window decorations (title bar, minimize, maximize, close buttons)
        self.root.overrideredirect(True)

        # Center the window on the screen
        self.root.eval('tk::PlaceWindow . center')

        # Header label: "Cipher" with smaller font
        header_label = tk.Label(
            self.root,
            text="Cipher",
            font=("Arial", 12, "bold"),
            fg="#000000",  # Black text
            bg="#FFFFFF",
            pady=2  # Reduced padding
        )
        header_label.pack(fill="x")

        # Message label with smaller font and tighter wrapping
        message_label = tk.Label(
            self.root,
            text=message,
            font=("Arial", 10),
            fg="#000000",  # Black text for white background
            bg="#FFFFFF",
            wraplength=250,  # Reduced wraplength for smaller width
            justify="center",
            pady=1  # Reduced padding
        )
        message_label.pack(expand=True, fill="x")

        # Calculate approximate height based on wrapped text
        char_count = len(message)
        lines = max(1, (char_count // 35) + 1)  # Estimate lines, ~35 chars per line
        text_height = lines * 15  # Approximate height per line (15 pixels per line)
        total_height = 30 + text_height + 10  # Header (20) + text + minimal padding (10)

        # Set window geometry with smaller width (300) and dynamic height
        self.root.geometry(f"300x{min(max(total_height, 80), 300)}")  # Min 80px, max 300px height

        # Automatically destroy the window after 2 seconds
        self.root.after(2000, self.root.destroy)

    def show(self):
        """Display the alert window and wait for it to close."""
        self.root.mainloop()

def show_alert(message="An unexpected error occurred."):
    """
    Helper function to show a compact custom alert with the specified message.
    
    Args:
        message (str): Message to display in the alert.
    """
    alert = CustomAlert(message=message)
    alert.show()