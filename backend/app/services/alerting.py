"""
Alerting service — evaluates alert rules and dispatches notifications
to configured channels (Slack, PagerDuty, email, webhook).
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import decrypt_credentials
from app.db.models.enterprise import AlertHistory, AlertRule, NotificationChannel

logger = logging.getLogger("edeltashared.alerting")


class AlertDispatcher:
    """Evaluates alert conditions and sends notifications."""

    async def evaluate_and_dispatch(
        self,
        session: AsyncSession,
        alert_type: str,
        severity: str,
        message: str,
        payload: dict,
        org_id: str | None = None,
    ) -> int:
        """Find matching alert rules and dispatch to their channels.

        Returns number of alerts sent.
        """
        stmt = select(AlertRule).where(
            AlertRule.is_enabled == True,
            AlertRule.condition_type == alert_type,
        )
        if org_id:
            stmt = stmt.where(AlertRule.org_id == org_id)

        result = await session.execute(stmt)
        rules = result.scalars().all()

        sent = 0
        now = datetime.now(timezone.utc)

        for rule in rules:
            # Check cooldown
            if rule.last_fired_at:
                cooldown_end = rule.last_fired_at + timedelta(minutes=rule.cooldown_minutes)
                if now < cooldown_end:
                    continue

            # Check severity filter
            if rule.severity_filter and not self._severity_matches(severity, rule.severity_filter):
                continue

            # Load channel
            ch_result = await session.execute(
                select(NotificationChannel).where(
                    NotificationChannel.id == rule.channel_id,
                    NotificationChannel.is_active == True,
                )
            )
            channel = ch_result.scalar_one_or_none()
            if channel is None:
                continue

            # Dispatch
            success = await self._send(channel, alert_type, severity, message, payload)

            # Record in history
            history = AlertHistory(
                rule_id=rule.id,
                channel_id=channel.id,
                alert_type=alert_type,
                severity=severity,
                message=message,
                payload=payload,
                delivery_status="sent" if success else "failed",
            )
            session.add(history)

            # Update last_fired_at
            rule.last_fired_at = now

            if success:
                sent += 1

        await session.commit()
        return sent

    async def _send(
        self,
        channel: NotificationChannel,
        alert_type: str,
        severity: str,
        message: str,
        payload: dict,
    ) -> bool:
        try:
            config = json.loads(decrypt_credentials(channel.config_encrypted))

            match channel.channel_type:
                case "slack":
                    return await self._send_slack(config, alert_type, severity, message)
                case "pagerduty":
                    return await self._send_pagerduty(config, alert_type, severity, message, payload)
                case "email":
                    return await self._send_email(config, alert_type, severity, message)
                case "webhook":
                    return await self._send_webhook(config, alert_type, severity, message, payload)
                case _:
                    logger.warning(f"Unknown channel type: {channel.channel_type}")
                    return False
        except Exception as e:
            logger.error(f"Failed to send alert via {channel.channel_type}: {e}")
            return False

    async def _send_slack(
        self, config: dict, alert_type: str, severity: str, message: str
    ) -> bool:
        webhook_url = config["webhook_url"]
        emoji = {"critical": ":red_circle:", "warning": ":warning:", "info": ":information_source:"}.get(severity, ":bell:")

        slack_message = {
            "text": f"{emoji} *eDeltaShared Alert — {alert_type}*\n{message}",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"{emoji} *{alert_type.upper()}*\n{message}"},
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"Severity: *{severity}* | Source: eDeltaShared CDC"},
                    ],
                },
            ],
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(webhook_url, json=slack_message, timeout=10)
            return resp.status_code == 200

    async def _send_pagerduty(
        self, config: dict, alert_type: str, severity: str, message: str, payload: dict
    ) -> bool:
        routing_key = config["routing_key"]
        pd_severity = {"critical": "critical", "warning": "warning"}.get(severity, "info")

        event = {
            "routing_key": routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"eDeltaShared: {message}",
                "severity": pd_severity,
                "source": "edeltashared-cdc",
                "component": alert_type,
                "custom_details": payload,
            },
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=event,
                timeout=10,
            )
            return resp.status_code == 202

    async def _send_email(
        self, config: dict, alert_type: str, severity: str, message: str
    ) -> bool:
        # Placeholder — would use SMTP or a service like Resend/SendGrid
        logger.info(
            f"Email alert to {config.get('to', 'unknown')}: "
            f"[{severity}] {alert_type} — {message}"
        )
        return True

    async def _send_webhook(
        self, config: dict, alert_type: str, severity: str, message: str, payload: dict
    ) -> bool:
        url = config["url"]
        body = {
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
            "payload": payload,
            "source": "edeltashared",
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=body, timeout=10)
            return 200 <= resp.status_code < 300

    def _severity_matches(self, event_severity: str, filter_severity: str) -> bool:
        levels = {"info": 0, "warning": 1, "critical": 2}
        return levels.get(event_severity, 0) >= levels.get(filter_severity, 0)


alert_dispatcher = AlertDispatcher()
