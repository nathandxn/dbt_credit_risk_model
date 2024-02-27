{#

test for checking if a column is not null with a condition that applies to the model 
specified in the .yml

example:

    - name: linked_email_id
      tests:
        - unique
        - not_null_when:
            condition: "source in ('email')"

#}

{% test not_null_when(model, column_name, condition) %}

    select *
    from {{ model }}
    where 1 = 1
        and {{ column_name }} is null
        and {{ condition }}

{% endtest %}
