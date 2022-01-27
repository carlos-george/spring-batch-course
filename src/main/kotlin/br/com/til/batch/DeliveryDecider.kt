package br.com.til.batch

import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import java.time.LocalDateTime

class DeliveryDecider : JobExecutionDecider {
    override fun decide(jobExecution: JobExecution, steoExecution: StepExecution?): FlowExecutionStatus {

        val hour = LocalDateTime.now().hour
        val result = if (hour in 8..18)  "PRESENT" else "NOT_PRESENT"
        println("Decider result is $result")
        return FlowExecutionStatus(result)
    }
}