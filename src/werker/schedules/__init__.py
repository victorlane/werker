"""Werker scheduling: declarations (registry), the @schedule decorator, and
the syncschedules/scheduler runtime helpers.

Lazily import submodules so `import werker.schedules` stays cheap at
app-load time; pull the pieces you need by path.
"""
