package com.theara.springbatch.config;

import com.theara.springbatch.entity.Student;
import lombok.NonNull;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student, Student> {

    @Override
    public Student process(@NonNull Student student) throws Exception {
        return student;
    }
}
