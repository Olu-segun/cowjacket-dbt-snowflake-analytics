{% macro log_run_results(results) %}
    {% if execute %}
        {% set env = target.name %}
        {% set invocation = invocation_id %}
        {% set started_at = run_started_at %}

        {% for res in results %}
            {% if res.node is not none and res.node.resource_type == 'model' %}

                {% set sql %}
                INSERT INTO LAUNCHPAD.AUDIT.DBT_RUN_RESULTS (
                    invocation_id,
                    run_started_at,
                    environment,
                    model_name,
                    resource_type,
                    status,
                    execution_time_sec,
                    rows_affected,
                    adapter_response,
                    message
                )
                SELECT
                    '{{ invocation }}',
                    '{{ started_at }}',
                    '{{ env }}',
                    '{{ res.node.name }}',
                    '{{ res.node.resource_type }}',
                    '{{ res.status }}',
                    {{ res.execution_time or 0 }},
                    {{ res.adapter_response.get('num_rows', 'null') if res.adapter_response else 'null' }},
                    PARSE_JSON('{{ res.adapter_response | tojson }}'),
                    '{{ res.message | replace("'", "''") if res.message else '' }}'
                {% endset %}

                {% do run_query(sql) %}

            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
