import os
import sys
from pathlib import Path
from datetime import datetime

from flask import flash
from flask import Flask
from flask import request
from flask import session
from flask import url_for
from flask import jsonify
from flask import redirect
from flask import render_template

# Add utilities to path
sys.path.append(str(Path(__file__) / "rdbms"))
sys.path.append(str(Path(__file__) / "utilities"))

# Import utilities
from email_utils import send_email  # noqa
from securities import is_valid_email  # noqa
from securities import sanitize_input  # noqa
import utilities.pesapal_payment as pesapal  # noqa
from authentication import is_admin_logged_in  # noqa

# Import our custom RDBMS and models
from models.room import Room  # noqa
from database import Database  # noqa
from models.booking import Booking  # noqa
from models.payment import Payment  # noqa
from models.customer import Customer  # noqa
from models.pesapal_interim_payment import PesapalInterimPayment  # noqa

# Create Flask app
app = Flask(__name__)

# Configuration
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "hjdfbhjdbvhgjbnhjvdcghdscvgfb"
)
app.config["PESAPAL_CONSUMER_KEY"] = os.environ.get(
    "PESAPAL_CONSUMER_KEY", "qkio1BGGYAXTu2JOfm7XSXNruoZsrqEW"
)
app.config["PESAPAL_CONSUMER_SECRET"] = os.environ.get(
    "PESAPAL_CONSUMER_SECRET", "osGQ364R49cXKeOYSpaOnT++rHs="
)
app.config["PESAPAL_BASE_URL"] = "https://cybqa.pesapal.com/pesapalv3"

# Initialize database and models
database = Database("resort_booking_db", "file", "data", "json")
customer_model = Customer(database)
room_model = Room(database)
booking_model = Booking(database)
payment_model = Payment(database)
interim_payment_model = PesapalInterimPayment(database)

# Simple admin session management
ADMIN_USERNAME = "MikeT"
ADMIN_PASSWORD = "Mike@123"


def admin_required(f):
    """Decorator to require admin login"""

    def decorated_function(*args, **kwargs):
        if not is_admin_logged_in():
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)

    decorated_function.__name__ = f.__name__
    return decorated_function


# ============ PUBLIC ROUTES ============
@app.route("/")
@app.route("/home")
def index():
    """Homepage showing available rooms"""
    rooms = room_model.get_available_rooms()

    # Group rooms by type
    room_types = {}
    for room in rooms:
        room_type = room["roomType"]
        if room_type not in room_types:
            room_types[room_type] = []
        room_types[room_type].append(room)

    return render_template("index.html", room_types=room_types)


@app.route("/rooms/<room_type>")
def room_details(room_type):
    """Show details for specific room type"""
    rooms = room_model.get_by_type(room_type)
    if not rooms:
        flash(f"Room type '{room_type}' not found", "error")
        return redirect(url_for("index"))

    return render_template(
        "room_details.html", rooms=rooms, room_type=room_type
    )


@app.route("/booking")
def booking():
    """Booking page"""
    rooms = room_model.get_available_rooms()

    # Group by type for display
    room_types = {}
    for room in rooms:
        room_type = room["roomType"]
        if room_type not in room_types:
            room_types[room_type] = {
                "name": room_type,
                "rooms": [],
                "price": room["pricePerNight"],
                "capacity": room["capacity"],
                "description": room["description"],
                "amenities": room["amenities"].split("||")
                if room["amenities"]
                else [],
                "features": room["features"].split("||")
                if room["features"]
                else [],
                "imageUrl": room["imageUrl"],
            }
        room_types[room_type]["rooms"].append(room)

    return render_template("booking.html", room_types=room_types)


@app.route("/api/check-availability")
def check_availability():
    """Check room availability for given dates"""
    try:
        check_in_str = request.args.get("checkIn")
        check_out_str = request.args.get("checkOut")

        if not check_in_str or not check_out_str:
            return (
                jsonify(
                    {"error": "Both check-in and check-out dates are required"}
                ),
                400,
            )

        try:
            check_in = datetime.strptime(check_in_str, "%Y-%m-%d").date()
            check_out = datetime.strptime(check_out_str, "%Y-%m-%d").date()
        except ValueError:
            return (
                jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}),
                400,
            )

        if check_in >= check_out:
            return (
                jsonify(
                    {"error": "Check-out date must be after check-in date"}
                ),
                400,
            )

        # Get all rooms
        all_rooms = room_model.get_available_rooms()

        # Check which rooms are booked during the requested period
        all_bookings = booking_model.get_all()
        booked_room_ids = set()

        for booking in all_bookings:
            if booking["status"] in ["confirmed", "pending"]:
                booking_check_in = datetime.strptime(
                    booking["checkInDate"], "%Y-%m-%d"
                ).date()
                booking_check_out = datetime.strptime(
                    booking["checkOutDate"], "%Y-%m-%d"
                ).date()

                # Check for date overlap
                if not (
                    check_out <= booking_check_in
                    or check_in >= booking_check_out
                ):
                    # Find room for this booking (simplified - assuming one room per booking)
                    # In a full system, you'd have booking_assignments table
                    booked_room_ids.add(
                        booking.get("roomId")
                    )  # This would need proper implementation

        # Filter available rooms
        available_rooms = [
            room for room in all_rooms if room["roomId"] not in booked_room_ids
        ]

        # Group by type
        room_types = {}
        for room in available_rooms:
            room_type = room["roomType"]
            if room_type not in room_types:
                room_types[room_type] = {
                    "name": room_type,
                    "available_count": 0,
                    "price": room["pricePerNight"],
                    "capacity": room["capacity"],
                    "description": room["description"],
                    "amenities": room["amenities"].split("||")
                    if room["amenities"]
                    else [],
                    "imageUrl": room["imageUrl"],
                }
            room_types[room_type]["available_count"] += 1

        return jsonify(
            {
                "success": True,
                "room_types": list(room_types.values()),
                "check_in": check_in_str,
                "check_out": check_out_str,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/payment/pesapal/iframe", methods=["POST"])
def pesapal_iframe():
    """Process booking and generate PesaPal iframe"""
    try:
        # Validate form data
        full_name = sanitize_input(request.form.get("fullName", ""))
        email = sanitize_input(request.form.get("email", ""))
        phone = sanitize_input(request.form.get("phone", ""))
        check_in = request.form.get("checkIn")
        check_out = request.form.get("checkOut")
        adults_count = request.form.get("adultsCount")
        children_count = request.form.get("childrenCount", "0")
        special_requests = sanitize_input(
            request.form.get("specialRequests", "")
        )
        room_id = request.form.get("roomId")

        if not all(
            [
                full_name,
                email,
                phone,
                check_in,
                check_out,
                adults_count,
                room_id,
            ]
        ):
            flash("Please fill in all required fields", "error")
            return redirect(url_for("booking"))

        if not is_valid_email(email):
            flash("Please enter a valid email address", "error")
            return redirect(url_for("booking"))

        # Get or create customer
        customer_data = {
            "fullName": full_name,
            "email": email,
            "phoneNumber": phone,
        }
        customer = customer_model.get_or_create_by_email(customer_data)

        # Get room details
        room = room_model.get_by_id(int(room_id))
        if not room:
            flash("Selected room is not available", "error")
            return redirect(url_for("booking"))

        # Calculate total amount
        check_in_date = datetime.strptime(check_in, "%Y-%m-%d").date()
        check_out_date = datetime.strptime(check_out, "%Y-%m-%d").date()
        nights = (check_out_date - check_in_date).days
        total_amount = room["pricePerNight"] * nights

        # Create booking
        booking_data = {
            "customerId": customer["customerId"],
            "username": full_name,
            "emailAddress": email,
            "phoneNumber": phone,
            "checkInDate": check_in,
            "checkOutDate": check_out,
            "adultsCount": int(adults_count),
            "childrenCount": int(children_count),
            "specialRequests": special_requests,
            "status": "pending",
        }
        booking = booking_model.create(booking_data)

        # Get PesaPal access token
        access_token_response = pesapal.get_access_token()
        access_token = access_token_response.get("token")

        if not access_token:
            flash(
                "Payment processing temporarily unavailable. Please try again later.",
                "warning",
            )
            return redirect(url_for("booking"))

        # Get IPN URL and ID
        ipn_url = url_for("pesapal_booking_payment_ipn", _external=True)
        ipn_response = pesapal.get_notification_id(access_token, ipn_url)
        ipn_id = ipn_response.get("ipn_id")

        if not ipn_id:
            flash(
                "Payment processing temporarily unavailable. Please try again later.",
                "warning",
            )
            return redirect(url_for("booking"))

        # Prepare name parts
        first_name, middle_name, last_name = pesapal.split_full_name(full_name)

        # Create payment request
        payment_request = {
            "amount": total_amount,
            "description": f'Room Booking #{booking["bookingId"]} - {room["roomType"]} Room',
            "callback_url": url_for(
                "pesapal_booking_payment_redirect", _external=True
            ),
            "notification_id": ipn_id,
            "email_address": email,
            "phone_number": phone,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
        }

        # Get iframe URL
        iframe_response = pesapal.get_merchant_order_url(
            payment_request, access_token
        )

        if not iframe_response.get("order_tracking_id"):
            flash(
                "Payment processing temporarily unavailable. Please try again later.",
                "warning",
            )
            return redirect(url_for("booking"))

        # Save interim payment details
        interim_payment_data = {
            "orderTrackingId": iframe_response.get("order_tracking_id"),
            "merchantReference": iframe_response.get("merchant_reference"),
            "bookingId": booking["bookingId"],
            "amount": total_amount,
            "iframeSrc": iframe_response.get("redirect_url"),
            "status": "SAVED",
        }
        interim_payment_model.create(interim_payment_data)

        # Store booking details in session for confirmation page
        session["current_booking"] = {
            "booking_id": booking["bookingId"],
            "room_type": room["roomType"],
            "room_number": room["roomNumber"],
            "check_in": check_in,
            "check_out": check_out,
            "nights": nights,
            "amount": total_amount,
            "guest_name": full_name,
        }

        return render_template(
            "payment_iframe.html",
            iframe_src=iframe_response.get("redirect_url"),
            booking=booking,
            room=room,
            total_amount=total_amount,
        )

    except Exception as e:
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for("booking"))


@app.route("/payment/pesapal/redirect/booking_payment")
def pesapal_booking_payment_redirect():
    """Handle redirect after payment"""
    order_tracking_id = request.args.get("OrderTrackingId")

    if not order_tracking_id:
        flash("Invalid payment response received.", "danger")
        return redirect(url_for("booking"))

    # Get access token
    access_token_response = pesapal.get_access_token()
    access_token = access_token_response.get("token")

    if not access_token:
        flash(
            "Unable to verify payment status. Please contact support.", "danger"
        )
        return redirect(url_for("booking"))

    # Get transaction status
    status_response = pesapal.get_transaction_status(
        order_tracking_id, access_token
    )
    payment_status = status_response.get("payment_status_description")

    # Get interim payment record
    interim_payment = interim_payment_model.get_by_order_tracking_id(
        order_tracking_id
    )

    if not interim_payment:
        flash("Payment record not found. Please contact support.", "danger")
        return redirect(url_for("booking"))

    if payment_status == "Completed":
        flash(
            f'Payment successful! Receipt number: {status_response.get("confirmation_code")}. '
            f'Amount: {status_response.get("currency")} {status_response.get("amount")} '
            f'via {status_response.get("payment_method")}',
            "success",
        )
        return render_template(
            "payment_success.html",
            booking_id=interim_payment["bookingId"],
            receipt_number=status_response.get("confirmation_code"),
        )

    elif payment_status == "Pending":
        flash(
            "Your payment is being processed. We will notify you once confirmed.",
            "warning",
        )
        return render_template(
            "payment_pending.html", booking_id=interim_payment["bookingId"]
        )

    else:  # Failed or Invalid
        flash(
            f'Payment failed: {status_response.get("message")}. Please try again or contact support.',
            "danger",
        )
        return redirect(url_for("booking"))


@app.route("/payment/pesapal/ipn/booking-payment", methods=["GET"])
def pesapal_booking_payment_ipn():
    """Handle Instant Payment Notification from PesaPal"""
    order_tracking_id = request.args.get("OrderTrackingId")

    if not order_tracking_id:
        return "Invalid notification received.", 400

    # Get access token
    access_token_response = pesapal.get_access_token()
    access_token = access_token_response.get("token")

    if not access_token:
        return "Cannot verify payment status.", 400

    # Get transaction status
    status_response = pesapal.get_transaction_status(
        order_tracking_id, access_token
    )
    payment_status = status_response.get("payment_status_description")

    if payment_status == "Completed":
        return handle_completed_payment(status_response, order_tracking_id)

    elif payment_status == "Failed":
        return handle_failed_payment(status_response, order_tracking_id)

    elif payment_status == "Pending":
        return "Payment still pending.", 200

    return "Unhandled payment status.", 400


def handle_completed_payment(status_response, order_tracking_id):
    """Handle completed payment notification"""
    interim_payment = interim_payment_model.get_by_order_tracking_id(
        order_tracking_id
    )

    if not interim_payment:
        return "Payment record not found.", 404

    booking = booking_model.get_by_id(interim_payment["bookingId"])

    if not booking:
        return "Booking not found.", 404

    # Check for duplicate payment
    receipt_number = status_response.get("confirmation_code")
    existing_payment = payment_model.get_by_receipt_number(receipt_number)

    if existing_payment:
        return "Payment already processed.", 200

    # Create payment record
    payment_data = {
        "bookingId": booking["bookingId"],
        "receiptNumber": receipt_number,
        "amount": status_response.get("amount"),
        "paymentMethod": f'PesaPal - {status_response.get("payment_method")}',
        "status": "successful",
    }
    payment_model.create(payment_data)

    # Update booking status
    booking_model.update(booking["bookingId"], {"status": "confirmed"})

    # Update interim payment status
    interim_payment_model.update(
        interim_payment["pesapalInterimPaymentId"], {"status": "COMPLETED"}
    )

    # Send confirmation email
    try:
        send_email(
            [booking["emailAddress"]],
            f'Booking Confirmation #{booking["bookingId"]}',
            "booking_confirmation",
            booking=booking,
            receipt_number=receipt_number,
        )
    except Exception as e:
        print(f"Failed to send confirmation email: {e}")

    return "Payment processed successfully.", 200


def handle_failed_payment(status_response, order_tracking_id):
    """Handle failed payment notification"""
    interim_payment = interim_payment_model.get_by_order_tracking_id(
        order_tracking_id
    )

    if not interim_payment:
        return "Payment record not found.", 404

    booking = booking_model.get_by_id(interim_payment["bookingId"])

    if booking:
        # Update booking status to cancelled
        booking_model.update(booking["bookingId"], {"status": "cancelled"})

    # Update interim payment status
    interim_payment_model.update(
        interim_payment["pesapalInterimPaymentId"], {"status": "FAILED"}
    )

    return "Failed payment recorded.", 200


@app.route("/booking-status")
def booking_status():
    """Check booking status with receipt number and email"""
    return render_template("booking_status.html")


@app.route("/api/booking-status", methods=["POST"])
def api_booking_status():
    """API endpoint to get booking status"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "Invalid request format"}), 400

        receipt_number = data.get("receiptNumber")
        email = data.get("email")

        if not receipt_number or not email:
            return (
                jsonify({"error": "Receipt number and email are required"}),
                400,
            )

        # Find payment by receipt number
        payment = payment_model.get_by_receipt_number(receipt_number)

        if not payment:
            return jsonify({"error": "Booking not found"}), 404

        # Get booking details
        booking = booking_model.get_by_id(payment["bookingId"])

        if not booking or booking["emailAddress"] != email:
            return jsonify({"error": "Booking not found"}), 404

        # Get customer details
        customer_model.get_by_id(booking["customerId"])

        return jsonify(
            {
                "success": True,
                "booking": {
                    "bookingId": booking["bookingId"],
                    "guestName": booking["username"],
                    "email": booking["emailAddress"],
                    "phone": booking["phoneNumber"],
                    "checkInDate": booking["checkInDate"],
                    "checkOutDate": booking["checkOutDate"],
                    "adults": booking["adultsCount"],
                    "children": booking["childrenCount"],
                    "specialRequests": booking["specialRequests"],
                    "status": booking["status"],
                    "dateCreated": booking["dateCreated"],
                },
                "payment": {
                    "receiptNumber": payment["receiptNumber"],
                    "amount": payment["amount"],
                    "paymentMethod": payment["paymentMethod"],
                    "status": payment["status"],
                    "processedAt": payment["processedAt"],
                },
            }
        )

    except Exception:
        return (
            jsonify(
                {"error": "An error occurred while processing your request"}
            ),
            500,
        )


# ============ ADMIN ROUTES ============
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    """Admin login page"""
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            session["admin_logged_in"] = True
            flash("Login successful", "success")
            return redirect(url_for("admin_dashboard"))
        else:
            flash("Invalid credentials", "error")

    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    """Admin logout"""
    session.pop("admin_logged_in", None)
    flash("Logged out successfully", "success")
    return redirect(url_for("admin_login"))


@app.route("/admin")
@app.route("/admin/dashboard")
@admin_required
def admin_dashboard():
    """Admin dashboard"""
    # Get all bookings
    all_bookings = booking_model.get_all()

    # Get confirmed bookings (exclude pending)
    confirmed_bookings = [b for b in all_bookings if b["status"] != "pending"]

    # Get all payments
    all_payments = payment_model.get_all()

    # Get all rooms
    all_rooms = room_model.get_all()

    # Get all customers
    all_customers = customer_model.get_all()

    return render_template(
        "admin_dashboard.html",
        bookings=confirmed_bookings,
        payments=all_payments,
        rooms=all_rooms,
        customers=all_customers,
    )


@app.route("/admin/bookings/<int:booking_id>/cancel", methods=["POST"])
@admin_required
def cancel_booking(booking_id):
    """Cancel a booking"""
    booking = booking_model.get_by_id(booking_id)

    if not booking:
        flash("Booking not found", "error")
        return redirect(url_for("admin_dashboard"))

    if booking["status"] not in ["pending", "confirmed"]:
        flash("Cannot cancel this booking", "error")
        return redirect(url_for("admin_dashboard"))

    # Update booking status
    success = booking_model.update(booking_id, {"status": "cancelled"})

    if success:
        flash("Booking cancelled successfully", "success")
    else:
        flash("Failed to cancel booking", "error")

    return redirect(url_for("admin_dashboard"))


@app.route("/admin/bookings/<int:booking_id>/delete", methods=["POST"])
@admin_required
def delete_booking(booking_id):
    """Delete a booking (only if pending or cancelled)"""
    booking = booking_model.get_by_id(booking_id)

    if not booking:
        flash("Booking not found", "error")
        return redirect(url_for("admin_dashboard"))

    if booking["status"] not in ["pending", "cancelled"]:
        flash("Cannot delete confirmed or completed bookings", "error")
        return redirect(url_for("admin_dashboard"))

    success = booking_model.delete(booking_id)

    if success:
        flash("Booking deleted successfully", "success")
    else:
        flash("Failed to delete booking", "error")

    return redirect(url_for("admin_dashboard"))


@app.route("/admin/filter-bookings")
@admin_required
def filter_bookings():
    """Filter bookings by date range and status"""
    try:
        start_date_str = request.args.get("startDate")
        end_date_str = request.args.get("endDate")
        status = request.args.get("status")

        all_bookings = booking_model.get_all()
        filtered_bookings = []

        for booking in all_bookings:
            # Filter by date range
            if start_date_str and end_date_str:
                booking_date = datetime.strptime(
                    booking["checkInDate"], "%Y-%m-%d"
                ).date()
                filter_start = datetime.strptime(
                    start_date_str, "%Y-%m-%d"
                ).date()
                filter_end = datetime.strptime(end_date_str, "%Y-%m-%d").date()

                if not (filter_start <= booking_date <= filter_end):
                    continue

            # Filter by status
            if status and booking["status"] != status:
                continue

            filtered_bookings.append(booking)

        return jsonify(
            {
                "success": True,
                "bookings": filtered_bookings,
                "count": len(filtered_bookings),
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============ ERROR HANDLERS ============
@app.errorhandler(404)
def not_found(error):
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template("500.html"), 500


# ============ TEMPLATE FUNCTIONS ============
@app.template_filter("currency")
def currency_filter(amount):
    """Format currency"""
    return f"KES {amount:,.2f}"


@app.context_processor
def inject_current_year():
    """Inject current year into templates"""
    return {"current_year": datetime.now().year}


# ============ APPLICATION STARTUP ============
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
