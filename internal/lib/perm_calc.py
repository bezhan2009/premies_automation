def calculate_bonus(base_bonus, call_center_score, test_score, salary):
    """
    Рассчитывает итоговую премию с учетом коэффициентов по Call Center и Tests.
    Премия не может превышать 1.5 оклада (salary).

    :param base_bonus: float, исходная премия в сомони
    :param call_center_score: float, баллы за Call Center (0-10)
    :param test_score: float, баллы за Tests (0-10)
    :param salary: float, оклад в сомони
    :return: итоговая премия в сомони
    """

    if 0 <= call_center_score <= 1:
        call_center_coef = -30
    elif 2 <= call_center_score <= 3:
        call_center_coef = -20
    elif 3 < call_center_score <= 5:
        call_center_coef = -10
    elif 5 < call_center_score <= 7:
        call_center_coef = 0
    elif 7 < call_center_score <= 9:
        call_center_coef = 10
    elif 9 < call_center_score <= 10:
        call_center_coef = 20
    else:
        raise ValueError("Call Center score out of range (0-10)")

    if 0 <= test_score <= 2:
        test_coef = -10
    elif 2 < test_score <= 4:
        test_coef = -5
    elif 4 < test_score <= 6:
        test_coef = 0
    elif 6 < test_score <= 8:
        test_coef = 5
    elif 8 < test_score <= 9:
        test_coef = 10
    elif 9 < test_score <= 10:
        test_coef = 15
    else:
        raise ValueError("Test score out of range (0-10)")

    total_coef = call_center_coef + test_coef

    final_bonus = base_bonus * (1 + total_coef / 100)

    # Ограничиваем премию полутора окладами
    max_bonus = 1.5 * salary
    if final_bonus > max_bonus:
        final_bonus = max_bonus

    return final_bonus
