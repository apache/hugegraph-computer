/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
; (function () {
    const API_PREFIX = '/api/v1';
    window.vermeer = {
        login: function () {
            const token = $('#admin_token').val();
            if (!token) {
                showDelModal('empty token');
                return;
            }

            const req = { token: token };
            postJson('/login', req,
                function (data) {
                    console.log(data.message);
                    location.reload(true);
                }
            );
        },
        queryGraphs: function () {
            const $t = $('#graphs_table');
            $t.empty();
            const fields = ['space_name', 'name', 'status', 'state', 'create_time',
                'update_time', 'use_out_edges', 'use_out_degree'];
            $t.append('<thead><tr/></thead>');
            const $tr = $t.find('thead tr');
            $.each(fields, function (index, field) {
                $tr.append($('<th/>').text(field));
            });

            const ok = function (data) {
                const $tb = $('<tbody/>');
                $t.append($tb);
                const rows = data.graphs;
                $.each(rows, function (index, row) {
                    $tb.append(toTableRow(fields, row));
                });
            };

            get('/graphs', ok);
        },
        queryTasks: function () {
            const $t = $('#tasks_table');
            $t.empty();
            const fields = ['id', 'space_name', 'graph_name', 'create_user', 'task_type',
                'status', 'state', 'create_time', 'start_time', 'update_time'];
            $t.append('<thead><tr/></thead>');
            const $tr = $t.find('thead tr');
            $.each(fields, function (index, field) {
                $tr.append($('<th/>').text(field));
            });

            const ok = function (data) {
                const $tb = $('<tbody/>');
                $t.append($tb);
                const rows = data.tasks;
                $.each(rows, function (index, row) {
                    $tb.append(toTableRow(fields, row));
                });
            };
            get('/tasks', ok);
        }

    };

    function toTableRow(fields, row) {
        const $row = $('<tr>');
        $.each(fields, function (index, field) {
            let value = '';

            if (field.endsWith('_time')) {
                value = formatDate(row[field]);
            } else {
                value = row[field];
            }

            const $span = $('<span/>').text(value);

            switch (value) {
                case 'error':
                    $span.addClass('badge badge-lg badge-danger');
                    break;
                case 'incomplete':
                    $span.addClass('badge badge-lg badge-warning');
                    break;
                case 'complete':
                case 'loaded':
                case 'disk':
                    $span.addClass('badge badge-lg badge-success');
            }

            const $td = $('<td>').append($span);
            $row.append($td);
        });
        return $row;
    }

    function showDelModal(text) {
        $('#msg-modal-msg').text(text);
        $('#msg-modal').modal('show');
    }

    function get(url, ok, error, caller) {
        ajax('GET', url, '', ok, error, caller);
    }

    function postJson(url, data, ok, error, caller) {
        ajax('POST', url, JSON.stringify(data), ok, error, caller);
    }

    function ajax(method, url, data, ok, error, caller) {
        $.ajax({
            url: API_PREFIX + url,
            type: method,
            data: data,
            contentType: 'application/json',
            success: function (response) {
                if (!ok) {
                    console.log('ajax request successful:', response);
                    return;
                }
                if (caller) {
                    ok.apply(caller, [response]);
                } else {
                    ok(response);
                }
            },
            error: function (err) {
                if (err.status === 401) {
                    showDelModal('Login First!');
                    return;
                } else {
                    console.log('ajax request failed:', err);
                }
                if (!error) {
                    let data = JSON.parse(err.responseText);
                    showDelModal(data.message);
                    return;
                }
                if (caller) {
                    error.apply(caller, [err]);
                } else {
                    error(err);
                }
            }
        });
    }

    function formatDate(inputDate) {
        const date = new Date(inputDate);
        const year = date.getFullYear();
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const day = date.getDate().toString().padStart(2, '0');
        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');
        const seconds = date.getSeconds().toString().padStart(2, '0');

        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }

})();

$(function () {
    vermeer.queryGraphs();
    vermeer.queryTasks();
});
