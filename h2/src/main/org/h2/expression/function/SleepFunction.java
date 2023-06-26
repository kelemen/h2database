package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.value.Value;
import org.h2.value.ValueNull;

public final class SleepFunction extends Function1 {
    public SleepFunction(Expression arg) {
        super(arg);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value argValue = arg.getValue(session);
        if (argValue != ValueNull.INSTANCE) {
            long sleepMs = Math.round(argValue.getDouble() * 1000.0);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return visitor.getType() != ExpressionVisitor.DETERMINISTIC && super.isEverything(visitor);
    }

    @Override
    public String getName() {
        return "SLEEP";
    }
}
