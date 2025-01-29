from flask import Blueprint, jsonify, render_template, request, redirect, url_for, flash, make_response
from .utils import get_username_from_session
from app import db
from models import User, Poem
import secrets
import logging

logger = logging.getLogger(__name__)

users_bp = Blueprint('users', __name__)


@users_bp.route('/profile/<username>', methods=['GET', 'POST'])
def profile(username):
    try:
        session_id = request.cookies.get('session_id')
        if not session_id:
            return redirect(url_for('auth.login'))

        username_from_session, is_verified = get_username_from_session(session_id)
        if not is_verified:
            flash('Invalid session. Please log in again.', 'error')
            return redirect(url_for('auth.login'))

        user = User.query.filter_by(username=username).first()
        if not user:
            return make_response(jsonify({"error": "User not found"}), 404)

        if request.method == 'POST' and username_from_session == user.username:
            csrf_token = request.form.get('csrf_token')
            if not csrf_token or csrf_token != request.cookies.get('csrf_token'):
                return make_response(jsonify({"error": "Invalid CSRF token"}), 403)
            if user.aboutMe:
                return make_response(jsonify({"error": "Too many edits. Try again tomorrow"}), 403)
            about_me = request.form.get('aboutMe').strip()
            if len(about_me) > 1000:
                flash("About Me field should be less than 1000 characters.", 'warning')
            else:
                user.aboutMe = about_me
                db.session.commit()
                flash('Profile updated successfully!', 'success')
                return redirect(url_for('users.profile', username=user.username))

        poems = Poem.query.filter_by(user_id=user.id).all()

        return render_template('profile.html', user=user, poems=poems,
                               session_username=username_from_session, session_csrf_token=user.csrf_token)

    except Exception as e:
        logger.exception(f'An error occurred: {e}')
        db.session.rollback()
        return make_response(jsonify({"error": f'An error occurred: {str(e)}'}), 500)


@users_bp.route('/profile/<username>/toggle_privacy', methods=['POST'])
def toggle_privacy(username):
    try:
        session_id = request.cookies.get('session_id')
        if not session_id:
            return redirect(url_for('auth.login'))

        username_from_session, is_verified = get_username_from_session(session_id)
        if not is_verified:
            flash('Invalid session. Please log in again.', 'error')
            return redirect(url_for('auth.login'))

        user = User.query.filter_by(username=username).first()
        if not user:
            return make_response(jsonify({"error": "User not found"}), 404)

        if username_from_session == user.username:
            csrf_token = request.form.get('csrf_token')
            if not csrf_token or csrf_token != request.cookies.get('csrf_token'):
                return make_response(jsonify({"error": "Invalid CSRF token"}), 403)

            user.isPrivate = not user.isPrivate
            db.session.commit()
            flash('Privacy setting updated successfully!', 'success')

        return redirect(url_for('users.profile', username=user.username))

    except Exception as e:
        logger.exception(f'An error occurred: {e}')
        db.session.rollback()
        return make_response(jsonify({"error": f'An error occurred: {str(e)}'}), 500)


@users_bp.route('/users/recent', methods=['GET'])
def get_recent_users():
    try:
        users = User.query.order_by(User.id.desc()).limit(500).all()

        usernames = [user.username for user in users]

        return jsonify({"usernames": usernames}), 200
    except Exception as e:
        logger.exception(f'An error occurred: {e}')
        db.session.rollback()
        return make_response(jsonify({"error": f'An error occurred: {str(e)}'}), 500)
