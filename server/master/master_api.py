# -*- coding: utf-8 -*-

from flask import Blueprint, jsonify

bp = Blueprint('api', __name__)

@bp.route('/assign')
