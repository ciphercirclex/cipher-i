import analysechart_m
import updateorders

def run_analysechart_m():
    """Run the analysechart_m script."""
    try:
        analysechart_m.main()
        print("analysechart_m completed.")
    except Exception as e:
        print(f"Error in analysechart_m: {e}")

def run_updateorders():
    """Run the updateorders script."""
    try:
        updateorders.main()
        print("updateorders completed.")
    except Exception as e:
        print(f"Error in updateorders: {e}")

def execute(mode="loop"):
    """Execute the scripts sequentially with the specified mode: 'loop' or 'once'."""
    if mode not in ["loop", "once"]:
        raise ValueError("Invalid mode. Use 'loop' or 'once'.")

    def run_sequential():
        """Helper function to run analysechart_m and updateorders sequentially."""
        run_analysechart_m()  # Run analysechart_m first
        run_updateorders()    # Run updateorders second
        run_analysechart_m()  # Run analysechart_m first
        run_updateorders()    # Run updateorders second
        print("Both scripts completed.")

    try:
        if mode == "loop":
            while True:
                run_sequential()
                print("Restarting...")
        else:  # mode == "once"
            run_sequential()
            print("Execution completed (once mode).")

    except Exception as e:
        print(f"Error in main loop: {e}")

if __name__ == "__main__":
    # Example: Change to "once" or "loop" as needed
    execute(mode="once")