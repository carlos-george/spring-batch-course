package br.com.til.batch

import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import java.util.*

class ReceiptDecider : JobExecutionDecider {
    override fun decide(jobExecution: JobExecution, steoExecution: StepExecution?): FlowExecutionStatus {

        val exitCode = if (Random().nextFloat() < 0.70f)  "CORRECT" else "INCORRECT"
        println("The intem delivered is: $exitCode")
        return FlowExecutionStatus(exitCode)
    }
}