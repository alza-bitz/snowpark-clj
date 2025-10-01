# API Coverage Report

Generated: {{generated-at}}

## Summary

- Total Java API methods: {{stats.coverage/left-count}}
- Supported methods: {{stats.coverage/inner-count}}  
- Wrapper functions: {{stats.coverage/right-count}}
- Coverage: {{stats.coverage/inner-count}}/{{stats.coverage/left-count}} ({{stats.coverage/coverage-pc}}%)

## Supported Methods

| Java Class | Method | Clojure Wrapper |
|------------|--------|-----------------|{% for r in supported-results %}
| [{{r.scanner/class}}]({{r.scanner/javadoc}}) | `{{r.coverage/method-sig}}` | {% if r.coverage/qual-name %}[{{r.coverage/qual-name}}]({{r.scanner/github}}){% else %}-{% endif %} |{% endfor %}

## All Methods (Full Coverage)

| Java Class | Method | Supported | Clojure Wrapper |
|------------|--------|:---------:|-----------------|{% for r in all-results %}
| [{{r.scanner/class}}]({{r.scanner/javadoc}}) | `{{r.coverage/method-sig}}` | {% if r.coverage/supported %}✓{% else %}✗{% endif %} | {% if r.coverage/qual-name %}[{{r.coverage/qual-name}}]({{r.scanner/github}}){% else %}-{% endif %} |{% endfor %}